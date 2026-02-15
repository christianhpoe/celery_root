# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import django
import pytest
from django.http import HttpResponse
from django.test import RequestFactory

from celery_root.components.web.views import graphs as graph_views
from celery_root.core.db.models import Task, TaskFilter, TaskRelation

if TYPE_CHECKING:
    from collections.abc import Iterator

    from celery_root.core.db import DbClient


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


class _DummyDb:
    def __init__(self, tasks: list[Task], relations: list[TaskRelation]) -> None:
        self._tasks = {task.task_id: task for task in tasks}
        self._relations = relations

    def get_task(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    def get_task_relations(self, _root_id: str) -> list[TaskRelation]:
        return list(self._relations)

    def get_tasks(self, _filters: TaskFilter | None = None) -> list[Task]:
        return list(self._tasks.values())


@contextmanager
def _open_db(db: _DummyDb) -> Iterator[_DummyDb]:
    yield db


def _fake_render(
    _request: object,
    _template: str,
    _context: dict[str, object],
    status: int | None = None,
) -> HttpResponse:
    return HttpResponse("ok", status=status or 200)


def test_graph_helpers() -> None:
    relation = TaskRelation(root_id="root", parent_id="root", child_id="child", relation="parent")
    edges = graph_views._build_edges([relation])
    assert edges

    kind_map = graph_views._node_kind_map(edges)
    assert isinstance(kind_map, dict)

    assert graph_views._truncate("abc", limit=2) == "ab..."

    task = Task(task_id="t1", name="demo", state="SUCCESS", started=datetime.now(UTC), finished=datetime.now(UTC))
    assert graph_views._duration_ms(task) is not None


def test_build_graph_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(UTC)
    tasks = [
        Task(task_id="root", name="demo", state="SUCCESS", root_id="root", finished=now),
        Task(task_id="child", name="demo", state="FAILURE", parent_id="root", root_id="root", finished=now),
    ]
    relations = [TaskRelation(root_id="root", parent_id="root", child_id="child", relation="parent")]
    db = _DummyDb(tasks, relations)

    payload = graph_views._build_graph_payload("root", cast("DbClient", db))
    assert payload["nodes"]
    assert payload["edges"]

    monkeypatch.setattr(graph_views, "open_db", lambda: _open_db(db))
    monkeypatch.setattr(graph_views, "render", _fake_render)
    monkeypatch.setattr(graph_views, "reverse", lambda *_args, **_kwargs: "/url")

    factory = RequestFactory()
    response = graph_views.task_graph(factory.get("/graph/"), "root")
    assert response.status_code == 200
