# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import logging
from datetime import UTC, datetime
from multiprocessing import Process, Queue
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

import pytest

from celery_root.components.metrics.base import BaseMonitoringExporter
from celery_root.config import (
    CeleryRootConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    LoggingConfigFile,
    McpConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
)
from celery_root.core.db.models import TaskEvent, TaskStats, WorkerEvent
from celery_root.core.process_manager import (
    ProcessManager,
    _ExporterProcess,
    _McpServerProcess,
    _metrics_url,
    _WebServerProcess,
)

if TYPE_CHECKING:
    from celery_root.core.registry import WorkerRegistry


class _DummyExporter(BaseMonitoringExporter):
    def __init__(self) -> None:
        self.task_events: list[TaskEvent] = []
        self.worker_events: list[WorkerEvent] = []
        self.stats: list[TaskStats] = []
        self.started = False
        self.stopped = False

    def serve(self) -> None:
        self.started = True

    def shutdown(self) -> None:
        self.stopped = True

    def on_task_event(self, event: TaskEvent) -> None:
        self.task_events.append(event)

    def on_worker_event(self, event: WorkerEvent) -> None:
        self.worker_events.append(event)

    def update_stats(self, stats: TaskStats) -> None:
        self.stats.append(stats)


class _DummyProcess:
    def __init__(self, *, alive: bool = False) -> None:
        self._alive = alive
        self.started = False
        self.name = "dummy"

    def is_alive(self) -> bool:
        return self._alive

    def start(self) -> None:
        self.started = True
        self._alive = True

    def stop(self) -> None:
        self._alive = False

    def join(self, timeout: float | None = None) -> None:
        _ = timeout
        self._alive = False

    def terminate(self) -> None:
        self._alive = False


class _DummyConf(dict[str, object]):
    def __getattr__(self, name: str) -> object | None:
        return self.get(name)

    def __setattr__(self, name: str, value: object) -> None:
        self[name] = value


class _DummyApp:
    def __init__(self, broker_url: str, backend: str) -> None:
        self.conf = _DummyConf({"broker_url": broker_url, "result_backend": backend})


class _DummyGroup:
    def __init__(self, broker_url: str, apps: list[_DummyApp]) -> None:
        self.broker_url = broker_url
        self.apps = apps


class _DummyRegistry:
    def __init__(self, groups: dict[str, _DummyGroup]) -> None:
        self._groups = groups

    def get_brokers(self) -> dict[str, _DummyGroup]:
        return dict(self._groups)


@pytest.fixture
def manager_config(tmp_path: Path) -> CeleryRootConfig:
    db_path = tmp_path / "root.db"
    return CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=db_path),
        frontend=FrontendConfig(host="127.0.0.1", port=5555),
        prometheus=PrometheusConfig(port=9000, prometheus_path="/metrics"),
        open_telemetry=OpenTelemetryConfig(endpoint="http://otel"),
        mcp=McpConfig(auth_key="token"),
    )


def test_metrics_url(manager_config: CeleryRootConfig) -> None:
    url = _metrics_url(manager_config)
    assert url.startswith("http://127.0.0.1")
    assert url.endswith("/metrics")


def test_exporter_drain_events() -> None:
    exporter = _DummyExporter()
    queue: Queue[object] = Queue()
    process = _ExporterProcess(lambda: exporter, CeleryRootConfig(), "test", event_queue=queue)
    logger = logging.getLogger(__name__)

    queue.put(TaskEvent(task_id="t1", name="demo", state="SUCCESS", timestamp=datetime.now(UTC)))
    queue.put(WorkerEvent(hostname="w1", event="worker-online", timestamp=datetime.now(UTC)))
    queue.put(TaskStats(count=1))

    process._drain_events(exporter, logger)
    assert exporter.task_events
    assert exporter.worker_events
    assert exporter.stats


def test_monitor_restarts_process(manager_config: CeleryRootConfig) -> None:
    registry = _DummyRegistry({"redis://": _DummyGroup("redis://", [_DummyApp("redis://", "backend")])})
    manager = ProcessManager(cast("WorkerRegistry", registry), manager_config, None)
    manager._process_factories["worker"] = cast("Any", lambda: _DummyProcess(alive=False))
    manager._processes["worker"] = cast("Process", _DummyProcess(alive=False))
    manager._monitor()
    assert cast("_DummyProcess", manager._processes["worker"]).started


def test_wait_for_db_socket(manager_config: CeleryRootConfig) -> None:
    registry = _DummyRegistry({})
    manager = ProcessManager(cast("WorkerRegistry", registry), manager_config, None)
    socket_path = Path(manager_config.database.rpc_address())
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path.unlink(missing_ok=True)
    socket_path.write_text("ready")
    manager._wait_for_db_socket(cast("Process", _DummyProcess(alive=True)))
    socket_path.unlink(missing_ok=True)


def test_build_processes(manager_config: CeleryRootConfig) -> None:
    group = _DummyGroup("redis://", [_DummyApp("redis://", "redis://backend")])
    registry = _DummyRegistry({"redis://": group})
    manager = ProcessManager(cast("WorkerRegistry", registry), manager_config, None)
    manager._build_processes()
    assert "db_manager" in manager._process_factories
    assert any(name.startswith("event_listener") for name in manager._process_factories)
    assert "reconciler" in manager._process_factories
    assert "web" in manager._process_factories
    assert "mcp" in manager._process_factories


def test_run_and_stop(monkeypatch: pytest.MonkeyPatch, manager_config: CeleryRootConfig) -> None:
    registry = _DummyRegistry({})
    manager = ProcessManager(cast("WorkerRegistry", registry), manager_config, None)
    manager._processes = cast("dict[str, Process]", {"worker": _DummyProcess(alive=True)})

    def _monitor() -> None:
        manager._stop_event.set()

    monkeypatch.setattr(manager, "start", lambda: None)
    monkeypatch.setattr(manager, "_write_statuses", lambda: None)
    monkeypatch.setattr(manager, "_monitor", _monitor)
    monkeypatch.setattr("celery_root.core.process_manager.set_settings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("celery_root.core.process_manager.configure_process_logging", lambda *_args, **_kwargs: None)

    manager.run()
    manager.stop()


def test_start_launches_processes(monkeypatch: pytest.MonkeyPatch, manager_config: CeleryRootConfig) -> None:
    registry = _DummyRegistry({})
    manager = ProcessManager(cast("WorkerRegistry", registry), manager_config, None)
    manager._process_factories = cast(
        "dict[str, Any]",
        {
            "db_manager": lambda: _DummyProcess(alive=False),
            "worker": lambda: _DummyProcess(alive=False),
        },
    )
    monkeypatch.setattr(manager, "_build_processes", lambda: None)
    monkeypatch.setattr(manager, "_wait_for_db_socket", lambda _proc: None)
    manager.start()
    assert cast("_DummyProcess", manager._processes["db_manager"]).started


def test_web_server_process_run(monkeypatch: pytest.MonkeyPatch, manager_config: CeleryRootConfig) -> None:
    proc = _WebServerProcess("127.0.0.1", 5555, manager_config)
    monkeypatch.setattr("celery_root.core.process_manager.set_settings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("celery_root.core.process_manager.configure_process_logging", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "celery_root.components.web.devserver.serve",
        lambda _host, _port, shutdown_event: shutdown_event.set(),
    )
    proc.run()


def test_exporter_process_run(monkeypatch: pytest.MonkeyPatch, manager_config: CeleryRootConfig) -> None:
    exporter = _DummyExporter()

    proc = _ExporterProcess(lambda: exporter, manager_config, "test")
    monkeypatch.setattr("celery_root.core.process_manager.set_settings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("celery_root.core.process_manager.configure_process_logging", lambda *_args, **_kwargs: None)

    def _drain(_exporter: _DummyExporter, _logger: logging.Logger) -> None:
        proc.stop()

    monkeypatch.setattr(proc, "_drain_events", _drain)
    proc.run()


def test_mcp_server_process_run(monkeypatch: pytest.MonkeyPatch, manager_config: CeleryRootConfig) -> None:
    proc = _McpServerProcess("127.0.0.1", 5557, manager_config)
    monkeypatch.setattr("celery_root.core.process_manager.set_settings", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("celery_root.core.process_manager.configure_process_logging", lambda *_args, **_kwargs: None)

    class _Server:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            self.should_exit = False

        def run(self) -> None:
            self.should_exit = True

    def _create_app() -> object:
        return object()

    monkeypatch.setattr("celery_root.core.process_manager.create_asgi_app", _create_app)
    monkeypatch.setattr("celery_root.core.process_manager.UvicornServer", _Server)
    proc.run()
