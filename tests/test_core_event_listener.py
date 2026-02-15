# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import logging
from datetime import UTC, datetime
from multiprocessing import Queue
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import Self

    import pytest
    from celery import Celery

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, LoggingConfigFile
from celery_root.core import event_listener as listener
from celery_root.core.db.models import TaskEvent, TaskRelation, WorkerEvent
from celery_root.core.db.rpc_client import DbRpcClient


class _DummyDb:
    def __init__(self) -> None:
        self.task_events: list[TaskEvent] = []
        self.worker_events: list[WorkerEvent] = []
        self.task_relations: list[TaskRelation] = []

    def store_task_event(self, event: TaskEvent) -> None:
        self.task_events.append(event)

    def store_worker_event(self, event: WorkerEvent) -> None:
        self.worker_events.append(event)

    def store_task_relation(self, event: TaskRelation) -> None:
        self.task_relations.append(event)


class _DummyControl:
    def __init__(self) -> None:
        self.enabled = False

    def enable_events(self) -> None:
        self.enabled = True


class _DummyConf(dict[str, object]):
    def __getattr__(self, name: str) -> object | None:
        return self.get(name)

    def __setattr__(self, name: str, value: object) -> None:
        self[name] = value


class _DummyApp:
    def __init__(self) -> None:
        self.conf = _DummyConf()
        self.control = _DummyControl()
        self.loader = self._Loader()

    class _Loader:
        def import_default_modules(self) -> None:
            return None

    @staticmethod
    def connection() -> object:
        class _Conn:
            def __enter__(self) -> Self:
                return self

            def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
                return None

        return _Conn()


def test_event_timestamp_helpers() -> None:
    ts = listener._event_timestamp({"timestamp": 123})
    assert ts.tzinfo is not None
    dt = datetime(2024, 1, 1, tzinfo=UTC)
    ts2 = listener._event_timestamp({"timestamp": dt})
    assert ts2.tzinfo is not None

    received = listener._event_received_timestamp({"local_received": 456})
    assert received.tzinfo is not None

    assert listener._parse_iso_datetime("2024-01-01T00:00:00Z") is not None
    assert listener._parse_iso_datetime("bad") is None


def test_event_field_helpers() -> None:
    assert listener._event_field({"a": 1}, "a") == 1
    assert listener._event_field({"a": 1}, "b") is None

    event: dict[str, object] = {"headers": {"root_id": "root"}}
    assert listener._event_id(event, "root_id") == "root"

    stamps = listener._extract_stamps({"headers": {"stamped_headers": ["x"], "x": "y"}})
    assert stamps == {"x": "y"}


def test_task_and_worker_event_handling() -> None:
    db = _DummyDb()
    queue: Queue[object] = Queue(maxsize=1)
    listener_instance = listener.EventListener("redis://", config=None, metrics_queues=[queue])
    listener_instance._db_client = cast("DbRpcClient", db)

    task_payload: dict[str, object] = {
        "type": "task-succeeded",
        "uuid": "task-1",
        "name": "demo.add",
        "args": "[1, 2]",
        "kwargs": "{}",
        "result": "3",
        "timestamp": 123,
    }
    listener_instance._handle_event(task_payload)
    assert db.task_events

    worker_payload: dict[str, object] = {
        "type": "worker-online",
        "hostname": "worker-1",
        "timestamp": 123,
    }
    listener_instance._handle_event(worker_payload)
    assert db.worker_events

    relation_payload: dict[str, object] = {
        "type": "task-relation",
        "root_id": "root",
        "parent_id": "root",
        "child_id": "child",
        "relation": "parent",
    }
    listener_instance._handle_event(relation_payload)
    assert db.task_relations


def test_emit_handles_full_queue() -> None:
    db = _DummyDb()
    queue: Queue[object] = Queue(maxsize=1)
    queue.put_nowait(object())
    listener_instance = listener.EventListener("redis://", config=None, metrics_queues=[queue])
    listener_instance._db_client = cast("DbRpcClient", db)

    listener_instance._emit(TaskEvent(task_id="t1", name="demo", state="SUCCESS", timestamp=datetime.now(UTC)))
    assert db.task_events


def test_configure_from_workers() -> None:
    primary = _DummyApp()
    secondary = _DummyApp()
    primary.conf.update({"accept_content": ["json"], "task_serializer": "json"})
    secondary.conf.update({"accept_content": ["yaml"], "task_serializer": "yaml"})

    listener._configure_from_workers(
        cast("Celery", primary),
        cast("tuple[Celery, ...]", (primary, secondary)),
        logging.getLogger(__name__),
    )
    accept = cast("list[str]", primary.conf.get("accept_content", []))
    assert "json" in accept


def test_enable_events_heartbeat() -> None:
    app = _DummyApp()
    instance = listener.EventListener("redis://", config=None)
    last = instance._maybe_enable_events(cast("Celery", app), 0.0)
    assert last > 0.0
    heartbeat = instance._maybe_log_heartbeat(100.0, 0.0)
    assert heartbeat == 100.0


def test_select_event_app_and_accept_content(monkeypatch: pytest.MonkeyPatch) -> None:
    app_primary = _DummyApp()
    app_primary.conf["broker_url"] = "amqp://old"
    app_primary.conf["accept_content"] = ["json"]
    app_primary.conf["task_serializer"] = "json"

    app_secondary = _DummyApp()
    app_secondary.conf["accept_content"] = ["yaml"]
    app_secondary.conf["result_serializer"] = "yaml"

    monkeypatch.setattr(listener, "_load_worker_apps", lambda *_args, **_kwargs: (app_primary, app_secondary))
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=Path("./logs")),
        database=DatabaseConfigSqlite(db_path=None),
        worker_import_paths=["demo"],
    )
    app, apps = listener._select_event_app(config, "amqp://new", logging.getLogger(__name__))
    assert app.conf.broker_url == "amqp://new"
    accept = listener._collect_accept_content(apps)
    assert "json" in accept


def test_listener_run_and_listen(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
    )
    instance = listener.EventListener("redis://", config=config)

    class _DummyClient:
        def close(self) -> None:
            return None

    monkeypatch.setattr(listener, "set_settings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(listener, "configure_process_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(DbRpcClient, "from_config", lambda *_args, **_kwargs: _DummyClient())
    monkeypatch.setattr(listener, "_select_event_app", lambda *_args, **_kwargs: (_DummyApp(), ()))
    monkeypatch.setattr(listener, "_configure_from_workers", lambda *_args, **_kwargs: None)

    def _fake_listen(self: listener.EventListener, _app: object, last: float) -> float:
        self.stop()
        return last

    monkeypatch.setattr(listener.EventListener, "_listen", _fake_listen)
    instance.run()

    stop_event = instance._stop_event

    class _DummyReceiver:
        def __init__(
            self,
            _connection: object,
            *,
            handlers: dict[str, Callable[[dict[str, object]], None]],
            app: object,
            on_iteration: Callable[[], None],
        ) -> None:
            self.handlers = handlers
            self.on_iteration = on_iteration
            self._app = app
            self.should_stop = False

        def capture(self, *, limit: int | None, timeout: float, wakeup: bool) -> None:
            _ = (limit, timeout, wakeup)
            handler = self.handlers.get("*")
            if handler:
                handler({"type": "worker-online", "hostname": "worker"})
            self.on_iteration()
            stop_event.set()

    monkeypatch.setattr(listener, "_ManagedEventReceiver", _DummyReceiver)
    stop_event.clear()
    instance._listen(cast("Celery", _DummyApp()), 0.0)
