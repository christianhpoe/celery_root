# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

import pytest

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, LoggingConfigFile
from celery_root.core import reconciler
from celery_root.core.db.models import BrokerQueueEvent, Task, TaskEvent, TaskFilter, Worker, WorkerEvent
from celery_root.core.db.rpc_client import DbRpcClient
from celery_root.core.engine.brokers.base import QueueInfo

if TYPE_CHECKING:
    from pathlib import Path

    from celery_root.core.registry import WorkerRegistry


class _DummyDb:
    def __init__(self) -> None:
        self.worker_events: list[WorkerEvent] = []
        self.task_events: list[TaskEvent] = []
        self.broker_events: list[BrokerQueueEvent] = []
        self.tasks_by_state: dict[str, list[Task]] = {}
        self.workers: list[Worker] = []

    def get_workers(self) -> list[Worker]:
        return list(self.workers)

    def store_worker_event(self, event: WorkerEvent) -> None:
        self.worker_events.append(event)

    def store_task_event(self, event: TaskEvent) -> None:
        self.task_events.append(event)

    def store_broker_queue_event(self, event: BrokerQueueEvent) -> None:
        self.broker_events.append(event)

    def get_tasks(self, filters: TaskFilter | None = None) -> list[Task]:
        state = filters.state if filters is not None else None
        if state is None:
            return []
        return list(self.tasks_by_state.get(state, []))


class _DummyConf(dict[str, object]):
    def __getattr__(self, name: str) -> object | None:
        return self.get(name)

    def __setattr__(self, name: str, value: object) -> None:
        self[name] = value


class _DummyBackend:
    def __init__(self, meta: dict[str, object]) -> None:
        self._meta = meta

    def get_task_meta(self, _task_id: str) -> dict[str, object]:
        return dict(self._meta)


class _DummyControl:
    def __init__(self, inspector: object | None) -> None:
        self._inspector = inspector

    def inspect(self, *_args: object, **_kwargs: object) -> object | None:
        return self._inspector


class _DummyApp:
    def __init__(self, name: str, broker_url: str, meta: dict[str, object], inspector: object | None = None) -> None:
        self.main = name
        self.conf = _DummyConf({"broker_url": broker_url})
        self.backend = _DummyBackend(meta)
        self.control = _DummyControl(inspector)


class _DummyRegistry:
    def __init__(self, app: _DummyApp) -> None:
        self._app = app

    def get_app(self, _name: str) -> _DummyApp:
        return self._app


@pytest.fixture
def recon_config(tmp_path: Path) -> CeleryRootConfig:
    db_path = tmp_path / "root.db"
    return CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=db_path),
    )


def test_helpers_and_worker_info(recon_config: CeleryRootConfig) -> None:
    instance = reconciler.Reconciler(recon_config)
    snapshot = ({"pool": {"max-concurrency": 4}}, None, None, None, ["t1"], "broker", "app")
    info = instance._build_worker_info(snapshot)
    assert info["pool"]
    assert info["active"] == 1

    assert reconciler._to_int("3") == 3
    assert reconciler._to_float("1.5") == 1.5
    assert reconciler._final_state({"status": "SUCCESS"}) == "SUCCESS"


def test_refresh_and_reconcile(recon_config: CeleryRootConfig) -> None:
    instance = reconciler.Reconciler(recon_config)
    db = _DummyDb()
    task = Task(task_id="t1", name="demo", state="STARTED")
    db.tasks_by_state["STARTED"] = [task]
    instance._db_client = cast("DbRpcClient", db)
    instance._refresh_tasks()
    assert instance._task_queue

    meta = {"status": "SUCCESS", "date_done": datetime.now(UTC), "runtime": 1.0}
    app = _DummyApp("demo", "redis://", meta)
    instance._apps = cast("tuple[Any, ...]", (app,))

    _found_meta, status = instance._find_final_meta(task)
    assert status == "SUCCESS"

    instance._task_queue.append(task)
    instance._reconcile_task_states()
    assert db.task_events


def test_poll_broker_stats(monkeypatch: pytest.MonkeyPatch, recon_config: CeleryRootConfig) -> None:
    instance = reconciler.Reconciler(recon_config)
    db = _DummyDb()
    instance._db_client = cast("DbRpcClient", db)
    app = _DummyApp("demo", "redis://", {"status": "SUCCESS"})
    instance._apps = cast("tuple[Any, ...]", (app,))
    instance._app_names = ["demo"]

    monkeypatch.setattr(reconciler, "list_queues", lambda _registry, _name: [QueueInfo("celery", 2, 1)])

    registry = _DummyRegistry(app)
    instance._poll_broker_stats(cast("WorkerRegistry", registry))
    assert db.broker_events


def test_poll_worker_stats(recon_config: CeleryRootConfig) -> None:
    hostname = "worker-1"

    class _Inspector:
        def stats(self) -> dict[str, object]:
            return {hostname: {"pool": {"max-concurrency": 4}}}

        def conf(self) -> dict[str, object]:
            return {hostname: {"task_default_queue": "celery"}}

        def active_queues(self) -> dict[str, object]:
            return {hostname: [{"name": "celery"}]}

        def active(self) -> dict[str, object]:
            return {hostname: [1, 2]}

        def registered(self) -> dict[str, object]:
            return {hostname: ["demo.add"]}

    instance = reconciler.Reconciler(recon_config)
    db = _DummyDb()
    db.workers = [Worker(hostname=hostname, status="ONLINE")]
    instance._db_client = cast("DbRpcClient", db)
    instance._apps = cast(
        "tuple[Any, ...]",
        (_DummyApp("demo", "redis://", {"status": "SUCCESS"}, inspector=_Inspector()),),
    )
    instance._app_names = ["demo"]
    instance._worker_names = [hostname]
    instance._worker_refresh_at = 0.0

    instance._poll_worker_stats()
    assert db.worker_events


def test_refresh_workers(recon_config: CeleryRootConfig) -> None:
    instance = reconciler.Reconciler(recon_config)
    db = _DummyDb()
    db.workers = [Worker(hostname="alpha", status="ONLINE")]
    instance._db_client = cast("DbRpcClient", db)
    instance._refresh_workers()
    assert instance._worker_names == ["alpha"]


def test_reconciler_run(monkeypatch: pytest.MonkeyPatch, recon_config: CeleryRootConfig) -> None:
    instance = reconciler.Reconciler(recon_config)

    class _DummyClient:
        def close(self) -> None:
            return None

    class _DummyRegistry:
        def __init__(self, _paths: list[str]) -> None:
            self._apps: tuple[_DummyApp, ...] = ()

        def get_apps(self) -> tuple[_DummyApp, ...]:
            return self._apps

    monkeypatch.setattr(reconciler, "set_settings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(reconciler, "configure_process_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(DbRpcClient, "from_config", lambda *_args, **_kwargs: _DummyClient())
    monkeypatch.setattr(reconciler, "WorkerRegistry", _DummyRegistry)

    def _poll_workers() -> None:
        instance.stop()

    monkeypatch.setattr(instance, "_poll_worker_stats", _poll_workers)
    monkeypatch.setattr(instance, "_poll_broker_stats", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(instance, "_reconcile_task_states", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(time, "sleep", lambda *_args, **_kwargs: None)

    instance.run()
