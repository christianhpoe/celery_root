# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Process orchestration for Celery Root."""

from __future__ import annotations

import functools
import logging
import socket
import threading
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from multiprocessing import Event, Process, Queue
from pathlib import Path
from queue import Empty
from typing import TYPE_CHECKING, Protocol, cast

from celery.apps.beat import Beat
from celery.beat import Service

from celery_root.config import set_settings
from celery_root.core.db.manager import DBManager
from celery_root.core.logging import LogQueueConfig, configure_subprocess_logging, log_level_name
from celery_root.optional import require_optional_scope
from celery_root.shared.redaction import redact_url_password

from .event_listener import EventListener
from .reconciler import Reconciler

if TYPE_CHECKING:
    from celery import Celery

    from celery_root.components.metrics.base import BaseMonitoringExporter
    from celery_root.config import CeleryRootConfig
    from celery_root.core.db.adapters.base import BaseDBController

    from .registry import WorkerRegistry

_MONITOR_INTERVAL = 1.0
_HEARTBEAT_INTERVAL = 60.0
_DB_READY_TIMEOUT = 10.0
_DB_READY_POLL = 0.05


class _UvicornConfigInstance(Protocol):
    """Protocol for uvicorn Config instances."""


class _UvicornConfigType(Protocol):
    def __call__(  # noqa: PLR0913
        self,
        *,
        app: object,
        host: str,
        port: int,
        log_level: str,
        access_log: bool,
        ws: str,
    ) -> _UvicornConfigInstance: ...


class _UvicornServerInstance(Protocol):
    should_exit: bool

    def run(self) -> None: ...


class _UvicornServerType(Protocol):
    def __call__(self, *, config: _UvicornConfigInstance) -> _UvicornServerInstance: ...


class _BeatServiceFactory(Protocol):
    def __call__(
        self,
        app: Celery,
        max_interval: int | None = None,
        schedule_filename: str | None = None,
        scheduler_cls: str | None = None,
    ) -> Service: ...


@dataclass(frozen=True, slots=True)
class _ExporterRuntimeConfig:
    component: str
    metrics_url: str | None
    event_queue: Queue[object] | None
    log_config: LogQueueConfig | None


type McpDependencies = tuple[_UvicornConfigType, _UvicornServerType, Callable[[], object]]

UvicornConfig: _UvicornConfigType | None = None
UvicornServer: _UvicornServerType | None = None
create_asgi_app: Callable[[], object] | None = None


def _metrics_url(config: CeleryRootConfig) -> str:
    frontend = config.frontend
    host = frontend.host if frontend is not None else "127.0.0.1"
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    prometheus = config.prometheus
    path = "/metrics" if prometheus is None else prometheus.prometheus_path
    port = 8001 if prometheus is None else prometheus.port
    return f"http://{host}:{port}{path}"


def _mcp_url(config: CeleryRootConfig) -> str:
    mcp = config.mcp
    host = "127.0.0.1" if mcp is None else mcp.host
    port = 5557 if mcp is None else mcp.port
    path = "/mcp/" if mcp is None else (mcp.path or "/mcp/")
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if not path.startswith("/"):
        path = f"/{path}"
    if path != "/" and not path.endswith("/"):
        path = f"{path}/"
    return f"http://{host}:{port}{path}"


def _load_mcp_dependencies() -> McpDependencies:
    from uvicorn import Config as _UvicornConfig  # noqa: PLC0415
    from uvicorn import Server as _UvicornServer  # noqa: PLC0415

    from celery_root.components.mcp.server import create_asgi_app as _create_asgi_app  # noqa: PLC0415

    config_type: _UvicornConfigType = (
        cast("_UvicornConfigType", _UvicornConfig) if UvicornConfig is None else UvicornConfig
    )
    server_type: _UvicornServerType = (
        cast("_UvicornServerType", _UvicornServer) if UvicornServer is None else UvicornServer
    )
    app_factory: Callable[[], object] = _create_asgi_app if create_asgi_app is None else create_asgi_app

    return (config_type, server_type, app_factory)


class _WebServerProcess(Process):
    def __init__(
        self,
        host: str,
        port: int,
        config: CeleryRootConfig,
        log_config: LogQueueConfig | None,
    ) -> None:
        """Create a web server process wrapper."""
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._root_config = config
        self._log_config = log_config
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the web server to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run the web UI development server."""
        set_settings(self._root_config)
        configure_subprocess_logging(self._log_config)
        logger = logging.getLogger(__name__)
        logger.info("Web server starting on %s:%s", self._host, self._port)
        logger.info("Web server DB RPC socket path: %s", self._root_config.database.rpc_socket_path)
        logger.info("Web server DB RPC auth enabled: %s", bool(self._root_config.database.rpc_auth_key))

        def _heartbeat() -> None:
            while True:
                logger.info("Web server heartbeat on %s:%s", self._host, self._port)
                time.sleep(_HEARTBEAT_INTERVAL)

        threading.Thread(target=_heartbeat, daemon=True).start()
        require_optional_scope("web")
        from celery_root.components.web import devserver  # noqa: PLC0415

        devserver.serve(self._host, self._port, shutdown_event=self._stop_event)
        logger.info("Web server stopped on %s:%s", self._host, self._port)


class _ExporterProcess(Process):
    def __init__(
        self,
        exporter_factory: Callable[[], BaseMonitoringExporter],
        config: CeleryRootConfig,
        runtime: _ExporterRuntimeConfig,
    ) -> None:
        """Create a process to host a monitoring exporter."""
        super().__init__(daemon=True)
        self._exporter_factory = exporter_factory
        self._root_config = config
        self._component = runtime.component
        self._metrics_url = runtime.metrics_url
        self._event_queue = runtime.event_queue
        self._log_config = runtime.log_config
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the exporter process to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run the exporter and keep it alive until stopped."""
        set_settings(self._root_config)
        configure_subprocess_logging(self._log_config)
        logger = logging.getLogger(__name__)
        logger.info("Exporter process starting (%s).", self._component)
        exporter = self._exporter_factory()
        exporter.serve()
        if self._metrics_url:
            print(f"Prometheus metrics available at {self._metrics_url}")  # noqa: T201
        last_heartbeat = time.monotonic()
        while not self._stop_event.is_set():
            self._drain_events(exporter, logger)
            now = time.monotonic()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                logger.info("Exporter heartbeat.")
                last_heartbeat = now
        logger.info("Exporter process stopping (%s).", self._component)
        exporter.shutdown()

    def _drain_events(self, exporter: BaseMonitoringExporter, logger: logging.Logger) -> None:
        if self._event_queue is None:
            time.sleep(1.0)
            return
        try:
            item = self._event_queue.get(timeout=1.0)
        except Empty:
            return
        self._handle_event(exporter, logger, item)
        while True:
            try:
                item = self._event_queue.get_nowait()
            except Empty:
                # multiprocessing.Queue uses a feeder thread; give it a brief
                # chance to flush buffered items before declaring empty.
                try:
                    item = self._event_queue.get(timeout=0.01)
                except Empty:
                    return
            self._handle_event(exporter, logger, item)

    @staticmethod
    def _handle_event(exporter: BaseMonitoringExporter, logger: logging.Logger, item: object) -> None:
        from celery_root.core.db.models import TaskEvent, TaskStats, WorkerEvent  # noqa: PLC0415

        try:
            if isinstance(item, TaskEvent):
                exporter.on_task_event(item)
            elif isinstance(item, WorkerEvent):
                exporter.on_worker_event(item)
            elif isinstance(item, TaskStats):
                exporter.update_stats(item)
        except Exception:  # pragma: no cover - defensive
            logger.exception("Exporter failed to process event %r", item)


class _RootBeat(Beat):
    """Beat runner that keeps Root's logging configuration."""

    def setup_logging(self, colorize: bool | None = None) -> None:  # noqa: ARG002,FBT001
        return


class _BeatProcess(Process):
    def __init__(
        self,
        app_path: str,
        config: CeleryRootConfig,
        log_config: LogQueueConfig | None,
    ) -> None:
        """Create a Beat process wrapper for a single Celery app."""
        super().__init__(daemon=True)
        self._app_path = app_path
        self._config = config
        self._log_config = log_config
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the beat process to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run Celery beat with the DB-backed scheduler."""
        set_settings(self._config)
        configure_subprocess_logging(self._log_config)
        logger = logging.getLogger(__name__)
        logger.info("Beat process starting for %s", self._app_path)

        from celery_root.core.registry import WorkerRegistry  # noqa: PLC0415

        registry = WorkerRegistry([self._app_path])
        apps = registry.get_apps()
        if not apps:
            logger.error("Beat process could not load app %s", self._app_path)
            return
        app = apps[0]
        app.conf.beat_scheduler = "celery_root.components.beat.db_scheduler:DatabaseScheduler"
        beat_config = self._config.beat
        if beat_config is not None and beat_config.db_refresh_seconds is not None:
            app.conf.beat_db_refresh_seconds = beat_config.db_refresh_seconds

        loglevel = log_level_name(self._log_config.level if self._log_config else None)
        beat = _RootBeat(app=app, loglevel=loglevel, quiet=True)
        beat.init_loader()
        beat.set_process_title()
        service_factory = cast("_BeatServiceFactory", Service)
        service = service_factory(app, beat.max_interval, beat.schedule, beat.scheduler_cls)

        def _watch_stop() -> None:
            self._stop_event.wait()
            logger.info("Beat process stopping for %s", self._app_path)
            service.stop()

        threading.Thread(target=_watch_stop, daemon=True).start()
        try:
            beat.install_sync_handler(service)
            if beat.socket_timeout:
                logger.debug("Setting default socket timeout to %r", beat.socket_timeout)
                socket.setdefaulttimeout(beat.socket_timeout)
            service.start()
        except Exception:  # pragma: no cover - defensive
            logger.exception("Beat process crashed for %s", self._app_path)
        finally:
            logger.info("Beat process stopped for %s", self._app_path)


class _McpServerProcess(Process):
    def __init__(
        self,
        host: str,
        port: int,
        config: CeleryRootConfig,
        log_config: LogQueueConfig | None,
    ) -> None:
        """Create a MCP server process wrapper."""
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._config = config
        self._log_config = log_config
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the MCP server to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run the MCP server."""
        set_settings(self._config)
        configure_subprocess_logging(self._log_config)
        logger = logging.getLogger(__name__)
        logger.info("MCP server starting on %s:%s", self._host, self._port)
        print(  # noqa: T201
            f"MCP server available at {_mcp_url(self._config)}",
        )

        logger.info("MCP server DB RPC socket path: %s", self._config.database.rpc_socket_path)
        logger.info("MCP server DB RPC auth enabled: %s", bool(self._config.database.rpc_auth_key))
        require_optional_scope("mcp")
        uvicorn_config, uvicorn_server, create_app = _load_mcp_dependencies()
        app = create_app()
        server_config = uvicorn_config(
            app=app,
            host=self._host,
            port=self._port,
            log_level=log_level_name(self._log_config.level if self._log_config else None),
            access_log=False,
            ws="websockets-sansio",
        )
        server = uvicorn_server(config=server_config)

        def _watch_stop() -> None:
            self._stop_event.wait()
            server.should_exit = True

        threading.Thread(target=_watch_stop, daemon=True).start()
        server.run()
        logger.info("MCP server stopped on %s:%s", self._host, self._port)


class ProcessManager:
    """Start/stop subprocesses and monitor liveness."""

    def __init__(
        self,
        registry: WorkerRegistry,
        config: CeleryRootConfig,
        controller_factory: Callable[[], BaseDBController] | None,
        log_config: LogQueueConfig | None = None,
    ) -> None:
        """Initialize the process manager with runtime dependencies."""
        self._registry = registry
        self._config = config
        self._controller_factory = controller_factory
        self._log_config = log_config
        self._logger = logging.getLogger(__name__)
        self._stop_event = Event()
        self._process_factories: dict[str, Callable[[], Process]] = {}
        self._processes: dict[str, Process] = {}

    def start(self) -> None:
        """Start all configured subprocesses."""
        self._build_processes()
        self._logger.info("ProcessManager launching %d subprocesses.", len(self._process_factories))
        db_factory = self._process_factories.get("db_manager")
        if db_factory is not None:
            self._logger.info("Starting process db_manager.")
            db_process = db_factory()
            self._processes["db_manager"] = db_process
            db_process.start()
            self._wait_for_db_socket(db_process)
        for name, factory in self._process_factories.items():
            if name == "db_manager":
                continue
            self._logger.info("Starting process %s.", name)
            self._processes[name] = factory()
            self._processes[name].start()

    def run(self) -> None:
        """Run the supervisor loop until stopped."""
        set_settings(self._config)
        self._logger.info("ProcessManager starting.")
        self._logger.info("DB RPC socket path: %s", self._config.database.rpc_socket_path)
        self._logger.info("DB RPC auth enabled: %s", bool(self._config.database.rpc_auth_key))
        self.start()
        last_heartbeat = time.monotonic()
        try:
            while not self._stop_event.is_set():
                self._monitor()
                now = time.monotonic()
                if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                    self._logger.info("ProcessManager heartbeat (%d processes).", len(self._processes))
                    last_heartbeat = now
                time.sleep(_MONITOR_INTERVAL)
        except KeyboardInterrupt:
            self._logger.info("ProcessManager interrupted; shutting down.")
            self.stop()
        self._logger.info("ProcessManager stopped.")

    def stop(self) -> None:
        """Stop all subprocesses and wait for shutdown."""
        self._stop_event.set()
        self._logger.info("ProcessManager stopping.")
        for process in self._processes.values():
            stopper = getattr(process, "stop", None)
            if callable(stopper):
                with suppress(Exception):  # pragma: no cover - defensive
                    stopper()
        for process in self._processes.values():
            process.join(timeout=5)
            if process.is_alive():
                self._logger.warning("Process %s still running; terminating.", process.name)
                process.terminate()
                process.join(timeout=5)
                if process.is_alive():
                    self._logger.warning("Process %s still running after terminate.", process.name)

    def _build_processes(self) -> None:
        self._process_factories.clear()
        self._process_factories["db_manager"] = functools.partial(
            DBManager,
            self._config,
            self._controller_factory,
            self._log_config,
        )
        metrics_queues: list[Queue[object]] = []
        backend_map: dict[str, str] = {}
        broker_groups = self._registry.get_brokers()
        for broker_url, group in broker_groups.items():
            redacted_broker_url = redact_url_password(broker_url) or broker_url
            backends = {str(app.conf.result_backend) if app.conf.result_backend else "" for app in group.apps}
            backend_label = next(iter(backends)) if len(backends) == 1 else "multiple"
            if backend_label and backend_label != "multiple":
                backend_label = redact_url_password(backend_label) or backend_label
            backend_map[redacted_broker_url] = backend_label
        if self._config.prometheus is not None:
            require_optional_scope("prometheus")
            from celery_root.components.metrics.prometheus import PrometheusExporter  # noqa: PLC0415

            metrics_url = _metrics_url(self._config)
            prometheus_queue: Queue[object] = Queue(self._config.event_queue_maxsize)
            metrics_queues.append(prometheus_queue)
            prometheus_runtime = _ExporterRuntimeConfig(
                component="prometheus",
                metrics_url=metrics_url,
                event_queue=prometheus_queue,
                log_config=self._log_config,
            )
            self._process_factories["prometheus"] = functools.partial(
                _ExporterProcess,
                functools.partial(
                    PrometheusExporter,
                    port=self._config.prometheus.port,
                    broker_backend_map=backend_map,
                    flower_compatibility=self._config.prometheus.flower_compatibility,
                ),
                self._config,
                prometheus_runtime,
            )
        if self._config.open_telemetry is not None:
            require_optional_scope("otel")
            from celery_root.components.metrics.opentelemetry import OTelExporter  # noqa: PLC0415

            otel_queue: Queue[object] = Queue(self._config.event_queue_maxsize)
            metrics_queues.append(otel_queue)
            otel_runtime = _ExporterRuntimeConfig(
                component="otel",
                metrics_url=None,
                event_queue=otel_queue,
                log_config=self._log_config,
            )
            self._process_factories["otel"] = functools.partial(
                _ExporterProcess,
                functools.partial(
                    OTelExporter,
                    service_name=self._config.open_telemetry.service_name,
                    endpoint=self._config.open_telemetry.endpoint,
                    broker_backend_map=backend_map,
                ),
                self._config,
                otel_runtime,
            )
        listener_counts: dict[str, int] = {}
        for broker_url in broker_groups:
            redacted_broker_url = redact_url_password(broker_url) or broker_url
            display_url = redacted_broker_url or "default"
            count = listener_counts.get(display_url, 0) + 1
            listener_counts[display_url] = count
            suffix = f"#{count}" if count > 1 else ""
            name = f"event_listener:{display_url}{suffix}"
            self._process_factories[name] = functools.partial(
                EventListener,
                broker_url,
                self._config,
                metrics_queues=tuple(metrics_queues),
                log_config=self._log_config,
            )
        self._process_factories["reconciler"] = functools.partial(
            Reconciler,
            self._config,
            self._log_config,
        )
        if self._config.beat is not None:
            if not self._config.worker_import_paths:
                self._logger.warning("Beat enabled but no worker import paths configured; skipping beat processes.")
            else:
                for path in self._config.worker_import_paths:
                    cleaned = str(path).strip()
                    if not cleaned:
                        continue
                    name = f"beat:{cleaned}"
                    self._process_factories[name] = functools.partial(
                        _BeatProcess,
                        cleaned,
                        self._config,
                        self._log_config,
                    )
        if self._config.frontend is not None:
            require_optional_scope("web")
            self._process_factories["web"] = functools.partial(
                _WebServerProcess,
                self._config.frontend.host,
                self._config.frontend.port,
                self._config,
                self._log_config,
            )
        if self._config.mcp is not None:
            require_optional_scope("mcp")
            self._process_factories["mcp"] = functools.partial(
                _McpServerProcess,
                self._config.mcp.host,
                self._config.mcp.port,
                self._config,
                self._log_config,
            )

    def _monitor(self) -> None:
        for name, process in list(self._processes.items()):
            if process.is_alive():
                continue
            if self._stop_event.is_set():
                continue
            self._logger.warning("Process %s stopped; restarting", name)
            factory = self._process_factories.get(name)
            if factory is None:
                continue
            replacement = factory()
            self._processes[name] = replacement
            replacement.start()
            self._logger.info("Process %s restarted.", name)

    def _wait_for_db_socket(self, process: Process) -> None:
        address = self._config.database.rpc_address()
        socket_path = Path(address)
        deadline = time.monotonic() + _DB_READY_TIMEOUT
        while time.monotonic() < deadline:
            if socket_path.exists():
                return
            if not process.is_alive():
                self._logger.warning("DBManager stopped before socket was ready.")
                return
            time.sleep(_DB_READY_POLL)
        self._logger.warning(
            "DBManager socket did not appear within %.1fs (%s).",
            _DB_READY_TIMEOUT,
            socket_path,
        )
