"""Process orchestration for Celery CnC."""

from __future__ import annotations

import functools
import logging
import threading
import time
from contextlib import suppress
from multiprocessing import Event, Process, Queue
from typing import TYPE_CHECKING

from uvicorn import Config as UvicornConfig
from uvicorn import Server as UvicornServer

from celery_cnc.config import set_settings
from celery_cnc.db.manager import DBManager
from celery_cnc.logging.setup import configure_process_logging
from celery_cnc.mcp.server import create_asgi_app
from celery_cnc.monitoring.otel import OTelExporter
from celery_cnc.monitoring.prometheus import PrometheusExporter
from celery_cnc.web import devserver

from .event_listener import EventListener

if TYPE_CHECKING:
    from collections.abc import Callable

    from celery_cnc.config import CeleryCnCConfig
    from celery_cnc.db.abc import BaseDBController
    from celery_cnc.monitoring.abc import BaseMonitoringExporter

    from .registry import WorkerRegistry

_MONITOR_INTERVAL = 1.0
_HEARTBEAT_INTERVAL = 60.0


class _WebServerProcess(Process):
    def __init__(self, host: str, port: int, config: CeleryCnCConfig) -> None:
        """Create a web server process wrapper."""
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._config = config
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the web server to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run the web UI development server."""
        set_settings(self._config)
        configure_process_logging(self._config, component="web")
        logger = logging.getLogger(__name__)
        logger.info("Web server starting on %s:%s", self._host, self._port)

        def _heartbeat() -> None:
            while True:
                logger.info("Web server heartbeat on %s:%s", self._host, self._port)
                time.sleep(_HEARTBEAT_INTERVAL)

        threading.Thread(target=_heartbeat, daemon=True).start()
        devserver.serve(self._host, self._port, shutdown_event=self._stop_event)
        logger.info("Web server stopped on %s:%s", self._host, self._port)


class _ExporterProcess(Process):
    def __init__(
        self,
        exporter_factory: Callable[[], BaseMonitoringExporter],
        config: CeleryCnCConfig,
        component: str,
    ) -> None:
        """Create a process to host a monitoring exporter."""
        super().__init__(daemon=True)
        self._exporter_factory = exporter_factory
        self._config = config
        self._component = component
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the exporter process to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run the exporter and keep it alive until stopped."""
        set_settings(self._config)
        configure_process_logging(self._config, component=self._component)
        logger = logging.getLogger(__name__)
        logger.info("Exporter process starting (%s).", self._component)
        exporter = self._exporter_factory()
        exporter.serve()
        last_heartbeat = time.monotonic()
        while not self._stop_event.is_set():
            time.sleep(1.0)
            now = time.monotonic()
            if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                logger.info("Exporter heartbeat.")
                last_heartbeat = now
        logger.info("Exporter process stopping (%s).", self._component)
        exporter.shutdown()


class _McpServerProcess(Process):
    def __init__(self, host: str, port: int, config: CeleryCnCConfig) -> None:
        """Create a MCP server process wrapper."""
        super().__init__(daemon=True)
        self._host = host
        self._port = port
        self._config = config
        self._stop_event = Event()

    def stop(self) -> None:
        """Signal the MCP server to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run the MCP server."""
        set_settings(self._config)
        configure_process_logging(self._config, component="mcp")
        logger = logging.getLogger(__name__)
        logger.info("MCP server starting on %s:%s", self._host, self._port)
        app = create_asgi_app()

        config = UvicornConfig(
            app=app,
            host=self._host,
            port=self._port,
            log_level=self._config.log_level.lower(),
            access_log=False,
        )
        server = UvicornServer(config=config)

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
        config: CeleryCnCConfig,
        controller_factory: Callable[[], BaseDBController],
    ) -> None:
        """Initialize the process manager with runtime dependencies."""
        self._registry = registry
        self._config = config
        self._controller_factory = controller_factory
        self._queue: Queue[object] = Queue(maxsize=config.event_queue_maxsize)
        self._stop_event = Event()
        self._logger = logging.getLogger(__name__)
        self._process_factories: dict[str, Callable[[], Process]] = {}
        self._processes: dict[str, Process] = {}

    def start(self) -> None:
        """Start all configured subprocesses."""
        self._build_processes()
        self._logger.info("ProcessManager launching %d subprocesses.", len(self._process_factories))
        for name, factory in self._process_factories.items():
            self._logger.info("Starting process %s.", name)
            self._processes[name] = factory()
            self._processes[name].start()

    def run(self) -> None:
        """Run the supervisor loop until stopped."""
        set_settings(self._config)
        configure_process_logging(self._config, component="process_manager")
        self._logger.info("ProcessManager starting.")
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

    def _build_processes(self) -> None:
        self._process_factories.clear()
        self._process_factories["db_manager"] = functools.partial(
            DBManager,
            self._queue,
            self._controller_factory,
            self._config,
        )
        for broker_url in self._registry.get_brokers():
            name = f"event_listener:{broker_url}"
            self._process_factories[name] = functools.partial(EventListener, broker_url, self._queue, self._config)
        if self._config.prometheus:
            self._process_factories["prometheus"] = functools.partial(
                _ExporterProcess,
                lambda: PrometheusExporter(port=self._config.prometheus_port),
                self._config,
                "prometheus",
            )
        if self._config.opentelemetry:
            self._process_factories["otel"] = functools.partial(
                _ExporterProcess,
                OTelExporter,
                self._config,
                "otel",
            )
        if self._config.web_enabled:
            self._process_factories["web"] = functools.partial(
                _WebServerProcess,
                self._config.web_host,
                self._config.web_port,
                self._config,
            )
        if self._config.mcp_enabled:
            self._process_factories["mcp"] = functools.partial(
                _McpServerProcess,
                self._config.mcp_host,
                self._config.mcp_port,
                self._config,
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
