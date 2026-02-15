# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import django
import pytest
from django.http import HttpResponse
from django.test import RequestFactory

from celery_root.components.web.views import tasks as task_views
from celery_root.core.db.models import Task, TaskFilter, TaskRelation, Worker
from celery_root.core.engine import tasks as task_control

if TYPE_CHECKING:
    from collections.abc import Iterator

    from celery_root.core.db import DbClient


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyDb:
    def __init__(
        self,
        tasks: list[Task],
        relations: list[TaskRelation],
        workers: list[Worker],
    ) -> None:
        self._tasks = tasks
        self._relations = relations
        self._workers = workers

    def get_tasks_page(
        self,
        _filters: TaskFilter | None,
        *,
        sort_key: str | None,
        sort_dir: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        tasks = list(self._tasks)
        if sort_key == "state":
            reverse = sort_dir == "desc"
            tasks.sort(key=lambda task: task.state, reverse=reverse)
        return tasks[offset : offset + limit], len(tasks)

    def get_tasks(self, _filters: TaskFilter | None = None) -> list[Task]:
        return list(self._tasks)

    def get_task(self, task_id: str) -> Task | None:
        for task in self._tasks:
            if task.task_id == task_id:
                return task
        return None

    def get_task_relations(self, _root_id: str) -> list[TaskRelation]:
        return list(self._relations)

    def get_workers(self) -> list[Worker]:
        return list(self._workers)

    def list_task_names(self) -> list[str]:
        return sorted({task.name or "unknown" for task in self._tasks})


class _DummyRegistry:
    def __init__(self, apps: list[object]) -> None:
        self._apps = apps

    def get_apps(self) -> list[object]:
        return list(self._apps)


class _DummyApp:
    def __init__(self, name: str) -> None:
        self.main = name
        self.conf: dict[str, object] = {}


@contextmanager
def _open_db(db: _DummyDb) -> Iterator[_DummyDb]:
    yield db


def _fake_render(_request: object, _template: str, context: dict[str, object]) -> HttpResponse:
    payload = json.dumps({"title": context.get("title")})
    return HttpResponse(payload, content_type="application/json")


def _make_tasks(now: datetime) -> list[Task]:
    return [
        Task(
            task_id="task-1",
            name="demo.add",
            state="SUCCESS",
            worker="alpha",
            received=now - timedelta(seconds=3),
            started=now - timedelta(seconds=2),
            finished=now - timedelta(seconds=1),
            runtime=1.5,
            args="[1, 2]",
            kwargs_="{}",
            result="3",
            traceback=None,
            retries=0,
        ),
        Task(
            task_id="task-2",
            name="demo.mul",
            state="FAILURE",
            worker="beta",
            received=now - timedelta(seconds=5),
            started=now - timedelta(seconds=4),
            finished=now - timedelta(seconds=3),
            runtime=2.5,
            args="[2, 3]",
            kwargs_="{}",
            result=None,
            traceback="boom",
            retries=1,
        ),
    ]


def test_parse_date_helpers() -> None:
    naive = "2024-01-02"
    parsed = task_views._parse_date(naive)
    assert parsed is not None
    assert parsed.tzinfo is not None
    assert task_views._is_date_only(naive)
    assert not task_views._is_date_only("2024-01-02T10:30:00")
    assert task_views._parse_date("not-a-date") is None


def test_parse_args_kwargs_and_strip() -> None:
    assert task_views._parse_args("[1, 2]") == [1, 2]
    assert task_views._parse_args("(1, 2)") == [1, 2]
    assert task_views._parse_args("bogus") == []
    assert task_views._parse_kwargs('{"x": 1}') == {"x": 1}
    assert task_views._parse_kwargs("{'x': 2}") == {"x": 2}
    assert task_views._parse_kwargs("bogus") == {}

    payload = {"a": [1, Ellipsis, {"b": Ellipsis}]}
    stripped = task_views._strip_ellipsis(payload)
    assert stripped == {"a": [1, None, {"b": None}]}


def test_annotation_parsing_helpers() -> None:
    assert task_views._split_annotation_args("Literal[1, 2, 3]") == ["Literal[1, 2, 3]"]
    assert task_views._normalize_annotation_text("Optional[int]") == "int"
    literal_info = task_views._annotation_info_from_text("Literal['a', 'b']")
    assert literal_info["input"] == "select"
    assert literal_info["options"] == ["a", "b"]

    info = task_views._annotation_info(list[int])
    assert info["input"] == "json-list"
    mapping_info = task_views._annotation_info(dict[str, int])
    assert mapping_info["input"] == "json-dict"


def test_task_signature_and_params() -> None:
    def sample(task: object, value: int, *, flag: bool = False) -> None:
        _ = (task, value, flag)

    class BoundTask:
        bind = True

        def run(self, task: object, value: int, *, flag: bool = False) -> None:
            _ = (task, value, flag)

    signature = task_views._task_signature(sample)
    assert signature is not None
    params = task_views._task_params(sample, signature)
    assert params[0]["name"] == "task"

    bound_sig = task_views._task_signature(BoundTask())
    assert bound_sig is not None
    bound_params = task_views._task_params(BoundTask(), bound_sig)
    assert bound_params[0]["name"] == "value"


def test_task_default_queue_and_schema() -> None:
    class _Task:
        def __init__(self) -> None:
            self.queue = "direct"
            self.options = {"queue": "option"}

        def _get_exec_options(self) -> dict[str, object]:
            return {"queue": "exec"}

    class _Conf(dict[str, object]):
        def get(self, key: str, default: object | None = None) -> object | None:
            return super().get(key, default)

    class _App:
        def __init__(self) -> None:
            self.conf = _Conf({"task_default_queue": "default"})
            self.tasks: dict[str, object] = {"demo.task": _Task()}

    task = _Task()
    app = _App()

    assert task_views._task_default_queue(task, app) == "direct"
    task.queue = ""
    assert task_views._task_default_queue(task, app) == "option"
    task.options = {}
    assert task_views._task_default_queue(task, app) == "exec"

    schemas = task_views._build_task_schemas(cast("tuple[Any, ...]", (app,)), ["demo.task", "missing.task"])
    assert schemas["demo.task"]["queue"]
    assert schemas["missing.task"]["params"] == []


def test_sort_and_filter_helpers() -> None:
    now = datetime.now(UTC)
    tasks = [task_views._task_to_view(task) for task in _make_tasks(now)]
    filtered = task_views._filter_tasks(tasks, "demo")
    assert len(filtered) == 2

    sort_key, sort_dir = task_views._normalize_sort("state", "desc")
    sorted_tasks = task_views._sort_tasks(tasks, sort_key, sort_dir)
    assert sorted_tasks[0]["state"] >= sorted_tasks[-1]["state"]

    rows = task_views._build_stats_rows(_make_tasks(now))
    assert any(row["name"] == "demo.add" for row in rows)


def test_sort_headers_and_stats_helpers() -> None:
    factory = RequestFactory()
    request = factory.get("/tasks/", {"sort": "state", "dir": "desc"})
    sort_key, sort_dir = task_views._normalize_sort("state", "desc")
    headers = task_views._build_sort_headers(request, sort_key, sort_dir)
    assert headers

    stats_sort_key, stats_sort_dir = task_views._normalize_stats_sort("count", "asc")
    stats_headers = task_views._build_stats_sort_headers(request, stats_sort_key, stats_sort_dir)
    assert stats_headers

    rows = [
        {
            "name": "a",
            "count": 2,
            "failure_rate": None,
            "retry_rate": None,
            "avg": None,
            "p95": None,
            "p99": None,
            "min": None,
            "max": None,
        },
        {
            "name": "b",
            "count": 1,
            "failure_rate": 10.0,
            "retry_rate": 0.0,
            "avg": 1.0,
            "p95": 1.0,
            "p99": 1.0,
            "min": 1.0,
            "max": 1.0,
        },
    ]
    sorted_rows = task_views._sort_stats_rows(rows, stats_sort_key, stats_sort_dir)
    assert sorted_rows

    assert task_views._percentile([], 0.5) is None


def test_relation_helpers(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    task = Task(task_id="root", name="demo", state="SUCCESS", finished=now)
    relations = [
        TaskRelation(root_id="root", parent_id="root", child_id="child", relation="parent"),
        TaskRelation(root_id="root", parent_id="root", child_id='["child2", "child3"]', relation="parent"),
    ]
    assert task_views._parent_id(task, relations) == "root"
    children = task_views._child_ids(task, relations)
    assert "child" in children

    class _Db:
        def get_task_relations(self, _root_id: str) -> list[TaskRelation]:
            return relations

        def get_task(self, task_id: str) -> Task | None:
            if task_id == "root":
                return task
            return None

    @contextmanager
    def _db_ctx() -> Iterator[_Db]:
        yield _Db()

    monkeypatch.setattr(task_views, "open_db", _db_ctx)

    tasks = [task_views._task_to_view(task)]
    task_views._attach_child_counts(tasks)
    assert tasks[0]["child_count"] == 3

    link = task_views._build_task_link(cast("DbClient", _Db()), "root")
    assert link["exists"]

    missing = task_views._build_task_link(cast("DbClient", _Db()), "missing")
    assert not missing["exists"]


def test_task_list_paged_and_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    tasks = _make_tasks(now)
    relations = [TaskRelation(root_id="task-1", parent_id="task-1", child_id="task-2", relation="parent")]
    workers = [Worker(hostname="alpha", status="ONLINE")]
    db = _DummyDb(tasks, relations, workers)

    monkeypatch.setattr(task_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(task_views, "render", _fake_render)

    factory = RequestFactory()
    request = factory.get(
        "/tasks/",
        {
            "state": "SUCCESS",
            "page": "1",
            "page_size": "10",
            "tab": "stats",
        },
    )
    response = task_views.task_list(request)
    assert response.status_code == 200

    request_multi = factory.get(
        "/tasks/",
        {
            "state": "SUCCESS,FAILURE",
            "page": "1",
            "page_size": "10",
            "sort": "state",
            "dir": "asc",
        },
    )
    response_multi = task_views.task_list(request_multi)
    assert response_multi.status_code == 200


def test_task_detail_and_actions(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    tasks = _make_tasks(now)
    relations = [TaskRelation(root_id="task-1", parent_id="task-1", child_id="task-2", relation="parent")]
    workers = [Worker(hostname="alpha", status="ONLINE")]
    db = _DummyDb(tasks, relations, workers)

    monkeypatch.setattr(task_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(task_views, "render", _fake_render)

    factory = RequestFactory()
    detail_request = factory.get("/tasks/task-1/")
    response = task_views.task_detail(detail_request, "task-1")
    assert response.status_code == 200

    dummy_app = _DummyApp("dummy")
    registry = _DummyRegistry([dummy_app])

    def _fake_send_task(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr(task_views, "get_registry", lambda: registry)
    monkeypatch.setattr(task_views, "list_task_names", lambda _apps: ["demo.add"])
    monkeypatch.setattr(task_views, "_resolve_task_entry", lambda _apps, _name: (object(), dummy_app))
    monkeypatch.setattr(task_control, "send_task", _fake_send_task)

    submit_request = factory.post(
        "/tasks/submit/",
        {
            "task_name": "demo.add",
            "args": "[1, 2]",
            "kwargs": "{}",
            "repeat": "2",
        },
    )
    submit_response = task_views.task_submit(submit_request)
    assert submit_response.status_code == 302

    monkeypatch.setattr(task_views, "_fetch_task", lambda _task_id: tasks[0])
    retry_request = factory.post("/tasks/task-1/retry/")
    retry_response = task_views.task_retry(retry_request, "task-1")
    assert retry_response.status_code == 302

    monkeypatch.setattr(task_control, "revoke", lambda *_args, **_kwargs: None)
    revoke_request = factory.post("/tasks/task-1/revoke/")
    revoke_response = task_views.task_revoke(revoke_request, "task-1")
    assert revoke_response.status_code == 302
