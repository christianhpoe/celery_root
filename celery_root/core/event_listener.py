# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Celery event listener process."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from multiprocessing import Event, Process, Queue
from queue import Full
from typing import TYPE_CHECKING

from celery import Celery
from celery.events import EventReceiver
from kombu.exceptions import OperationalError
from pydantic import BaseModel

from celery_root.config import set_settings
from celery_root.core.db.models import TaskEvent, TaskRelation, WorkerEvent
from celery_root.core.db.rpc_client import DbRpcClient, RpcCallError
from celery_root.core.logging.setup import configure_process_logging
from celery_root.core.logging.utils import sanitize_component
from celery_root.core.registry import WorkerRegistry
from celery_root.shared.redaction import redact_access_data, redact_url_password

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from kombu.connection import Connection

    from celery_root.config import CeleryRootConfig

_TASK_STATE_MAP = {
    "task-received": "RECEIVED",
    "task-started": "STARTED",
    "task-succeeded": "SUCCESS",
    "task-failed": "FAILURE",
    "task-retried": "RETRY",
    "task-revoked": "REVOKED",
    "task-rejected": "REJECTED",
    "task-sent": "PENDING",
}

_ENABLE_EVENTS_INTERVAL = 30.0
_HEARTBEAT_INTERVAL = 60.0
_CAPTURE_TIMEOUT = 1.0
_SERIALIZER_KEYS = ("event_serializer", "task_serializer", "result_serializer")
_BROKER_KEYS = (
    "broker_use_ssl",
    "broker_transport_options",
    "broker_connection_retry_on_startup",
    "broker_connection_max_retries",
    "broker_connection_timeout",
    "broker_heartbeat",
    "broker_heartbeat_checkrate",
    "broker_pool_limit",
    "broker_failover_strategy",
)


def _redact_broker_url(value: str | None) -> str:
    if value is None:
        return ""
    redacted = redact_url_password(str(value))
    return redacted if redacted is not None else ""


def _load_worker_apps(
    config: CeleryRootConfig,
    broker_url: str,
    logger: logging.Logger,
) -> tuple[Celery, ...]:
    if not config.worker_import_paths:
        return ()
    try:
        registry = WorkerRegistry(config.worker_import_paths)
    except (ImportError, TypeError, ValueError) as exc:
        logger.warning("Failed to load worker apps for config sync: %s", exc)
        return ()
    group = registry.get_brokers().get(broker_url)
    if group is None:
        logger.warning(
            "No worker apps found for broker %s; using defaults.",
            _redact_broker_url(broker_url),
        )
        return ()
    return tuple(group.apps)


def _describe_app(app: Celery) -> str:
    raw_main = getattr(app, "main", None)
    if raw_main:
        return str(raw_main)
    conf_main = app.conf.get("main")
    if conf_main:
        return str(conf_main)
    return repr(app)


def _prime_app(app: Celery, logger: logging.Logger) -> None:
    try:
        app.loader.import_default_modules()
    except (ImportError, ModuleNotFoundError) as exc:  # pragma: no cover - defensive
        logger.warning("EventListener failed to import default modules for %s: %s", _describe_app(app), exc)


def _select_event_app(
    config: CeleryRootConfig,
    broker_url: str,
    logger: logging.Logger,
) -> tuple[Celery, tuple[Celery, ...]]:
    apps = _load_worker_apps(config, broker_url, logger)
    if not apps:
        app = Celery(broker=broker_url)
        _prime_app(app, logger)
        return app, ()
    for app in apps:
        _prime_app(app, logger)
    primary = apps[0]
    if broker_url and str(primary.conf.broker_url or "") != broker_url:
        logger.warning(
            "EventListener overriding worker app broker_url (%s) with %s.",
            _redact_broker_url(str(primary.conf.broker_url or "")),
            _redact_broker_url(broker_url),
        )
        primary.conf.broker_url = broker_url
    return primary, apps


def _collect_accept_content(apps: Sequence[Celery]) -> tuple[str, ...]:
    allowed: set[str] = set()
    for app in apps:
        raw_accept = app.conf.get("accept_content")
        if raw_accept:
            for item in raw_accept:
                allowed.add(str(item))
        for key in _SERIALIZER_KEYS:
            serializer = app.conf.get(key)
            if serializer:
                allowed.add(str(serializer))
    return tuple(sorted(allowed))


def _resolve_shared_setting(apps: Sequence[Celery], key: str) -> tuple[object | None, bool]:
    values: list[object] = []
    for app in apps:
        value = app.conf.get(key)
        if value is None:
            continue
        values.append(value)
    if not values:
        return None, False
    unique: list[object] = []
    for value in values:
        if not any(value == existing for existing in unique):
            unique.append(value)
    conflict = len(unique) > 1
    primary_value = apps[0].conf.get(key)
    if primary_value is None:
        primary_value = values[0]
    return primary_value, conflict


def _apply_setting(app: Celery, apps: Sequence[Celery], key: str, logger: logging.Logger) -> None:
    value, conflict = _resolve_shared_setting(apps, key)
    if value is None:
        return
    app.conf[key] = value
    if conflict:
        logger.warning(
            "EventListener using %s=%r from primary worker app; workers disagree.",
            key,
            redact_access_data(value),
        )


def _configure_from_workers(
    app: Celery,
    apps: Sequence[Celery],
    logger: logging.Logger,
) -> None:
    if not apps:
        return
    accept_content = _collect_accept_content(apps)
    if accept_content:
        app.conf.accept_content = list(accept_content)
        logger.info("EventListener accept_content: %s", ", ".join(accept_content))
    for key in _SERIALIZER_KEYS:
        _apply_setting(app, apps, key, logger)
    for key in _BROKER_KEYS:
        _apply_setting(app, apps, key, logger)


class _ManagedEventReceiver(EventReceiver):
    """Event receiver that delegates per-iteration bookkeeping."""

    def __init__(
        self,
        channel: Connection,
        *,
        handlers: dict[str, object],
        app: Celery,
        on_iteration: Callable[[], None],
    ) -> None:
        super().__init__(channel, handlers=handlers, app=app)
        self._on_iteration = on_iteration

    def on_iteration(self) -> None:
        self._on_iteration()


class EventListener(Process):
    """Listen to Celery events for a single broker."""

    def __init__(
        self,
        broker_url: str,
        config: CeleryRootConfig | None = None,
        metrics_queues: Sequence[Queue[object]] | None = None,
    ) -> None:
        """Create an event listener for a broker URL."""
        super().__init__(daemon=True)
        self.broker_url = broker_url
        self._broker_url_redacted = _redact_broker_url(broker_url)
        self._metrics_queues = list(metrics_queues or [])
        self._config = config
        self._stop_event = Event()
        self._logger = logging.getLogger(__name__)
        self._db_client: DbRpcClient | None = None

    def stop(self) -> None:
        """Signal the listener to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Listen for events and forward them to the DB manager and metrics queues."""
        component = f"event_listener-{sanitize_component(self._broker_url_redacted)}"
        if self._config is not None:
            set_settings(self._config)
            configure_process_logging(self._config, component=component)
        else:
            configure_process_logging(component=component)
        self._logger.info("EventListener starting for %s", self._broker_url_redacted)
        if self._config is not None:
            self._logger.info(
                "EventListener DB RPC socket path: %s",
                self._config.database.rpc_socket_path,
            )
            self._logger.info(
                "EventListener DB RPC auth enabled: %s",
                bool(self._config.database.rpc_auth_key),
            )
        if self._config is not None:
            self._db_client = DbRpcClient.from_config(self._config, client_name=component)
        worker_apps: tuple[Celery, ...] = ()
        if self._config is not None:
            app, worker_apps = _select_event_app(self._config, self.broker_url, self._logger)
        else:
            app = Celery(broker=self.broker_url)
            _prime_app(app, self._logger)
        _configure_from_workers(app, worker_apps, self._logger)
        last_heartbeat = time.monotonic()
        while not self._stop_event.is_set():
            try:
                last_heartbeat = self._listen(app, last_heartbeat)
            except (OperationalError, OSError) as exc:
                self._logger.warning("EventListener reconnecting to %s: %s", self._broker_url_redacted, exc)
                time.sleep(1.0)
            except KeyboardInterrupt:
                self._logger.info("EventListener interrupted for %s; stopping.", self._broker_url_redacted)
                self._stop_event.set()
            except Exception:  # pragma: no cover - defensive
                self._logger.exception("EventListener error for %s", self._broker_url_redacted)
                time.sleep(1.0)
        self._logger.info("EventListener stopped for %s", self._broker_url_redacted)
        if self._db_client is not None:
            self._db_client.close()

    def _listen(self, app: Celery, last_heartbeat: float) -> float:
        with app.connection() as connection:
            self._logger.info("EventListener connected to %s", self._broker_url_redacted)
            last_enable = 0.0
            last_heartbeat_box = [last_heartbeat]
            last_enable_box = [last_enable]

            def _on_iteration() -> None:
                now = time.monotonic()
                last_heartbeat_box[0] = self._maybe_log_heartbeat(now, last_heartbeat_box[0])
                last_enable_box[0] = self._maybe_enable_events(app, last_enable_box[0])
                if self._stop_event.is_set():
                    receiver.should_stop = True

            receiver = _ManagedEventReceiver(
                connection,
                handlers={"*": self._handle_event},
                app=app,
                on_iteration=_on_iteration,
            )
            try:
                while not self._stop_event.is_set():
                    try:
                        receiver.capture(limit=None, timeout=_CAPTURE_TIMEOUT, wakeup=True)
                    except TimeoutError:
                        continue
            finally:
                self._logger.info("EventListener disconnected from %s", self.broker_url)
        return last_heartbeat_box[0]

    def _maybe_log_heartbeat(self, now: float, last_heartbeat: float) -> float:
        if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
            self._logger.info("EventListener heartbeat for %s", self._broker_url_redacted)
            return now
        return last_heartbeat

    def _maybe_enable_events(self, app: Celery, last_enable: float) -> float:
        if time.monotonic() - last_enable < _ENABLE_EVENTS_INTERVAL:
            return last_enable
        try:
            app.control.enable_events()
        except Exception:  # pragma: no cover - best effort to enable events  # noqa: BLE001
            self._logger.debug("Failed to enable worker events for %s", self._broker_url_redacted)
            return last_enable
        else:
            now = time.monotonic()
            self._logger.info("Enabled events for workers on %s", self._broker_url_redacted)
            return now

    def _handle_event(self, event: dict[str, object]) -> None:
        event_type = str(event.get("type", ""))
        if event_type:
            self._logger.debug("Event received from %s: %s", self._broker_url_redacted, event_type)
        if event_type == "task-relation":
            self._handle_task_relation(event)
        elif event_type.startswith("task-"):
            self._handle_task_event(event_type, event)
        elif event_type.startswith("worker-"):
            self._handle_worker_event(event_type, event)

    def _handle_task_event(self, event_type: str, event: dict[str, object]) -> None:
        task_id = event.get("uuid") or event.get("id")
        if not task_id:
            return
        name = _event_field(event, "name", "task", "task_name")
        args = _event_field(event, "args", "argsrepr")
        kwargs = _event_field(event, "kwargs", "kwargsrepr")
        result = _event_field(event, "result", "exception")
        task_event = TaskEvent(
            task_id=str(task_id),
            name=_stringify(name),
            state=_TASK_STATE_MAP.get(event_type, event_type.replace("task-", "").upper()),
            timestamp=_event_timestamp(event),
            worker=_stringify(event.get("hostname")),
            args=_stringify(args),
            kwargs_=_stringify(kwargs),
            result=_stringify(result),
            traceback=_stringify(event.get("traceback")),
            stamps=_stringify(_extract_stamps(event)),
            runtime=_to_float(event.get("runtime")),
            retries=_to_int(event.get("retries")),
            eta=_parse_iso_datetime(event.get("eta")),
            expires=_parse_iso_datetime(event.get("expires")),
            parent_id=_stringify(_event_id(event, "parent_id", "parent")),
            root_id=_stringify(_event_id(event, "root_id", "root")),
            group_id=_stringify(_event_id(event, "group_id", "group")),
            chord_id=_stringify(_event_id(event, "chord_id", "chord")),
        )
        self._emit(task_event)

    def _handle_worker_event(self, event_type: str, event: dict[str, object]) -> None:
        hostname = event.get("hostname")
        if not hostname:
            return
        if event_type in {"worker-online", "worker-offline"}:
            self._logger.info("Worker %s event from %s: %s", hostname, self._broker_url_redacted, event_type)
        raw_info = {key: value for key, value in event.items() if key not in {"type", "timestamp"}}
        redacted_info = redact_access_data(raw_info)
        info = _json_safe(redacted_info) if isinstance(redacted_info, dict) else _json_safe({"value": redacted_info})
        worker_event = WorkerEvent(
            hostname=str(hostname),
            event=event_type,
            timestamp=_event_received_timestamp(event),
            info=info,
            broker_url=self._broker_url_redacted,
        )
        self._emit(worker_event)

    def _handle_task_relation(self, event: dict[str, object]) -> None:
        root_id = _stringify(event.get("root_id"))
        child_id = _stringify(event.get("child_id"))
        relation = _stringify(event.get("relation"))
        if not root_id or not child_id or not relation:
            return
        parent_id = _stringify(event.get("parent_id"))
        self._emit(
            TaskRelation(
                root_id=root_id,
                parent_id=parent_id,
                child_id=child_id,
                relation=relation,
            ),
            fanout=False,
        )

    def _emit(self, item: object, *, fanout: bool = True) -> None:
        self._send_to_db(item)
        if not fanout or not self._metrics_queues:
            return
        for queue in self._metrics_queues:
            try:
                queue.put_nowait(item)
            except Full:
                self._logger.debug("Dropping event for full metrics queue: %s", type(item).__name__)

    def _send_to_db(self, item: object) -> None:
        if self._db_client is None:
            return
        try:
            if isinstance(item, TaskEvent):
                self._db_client.store_task_event(item)
            elif isinstance(item, WorkerEvent):
                self._db_client.store_worker_event(item)
            elif isinstance(item, TaskRelation):
                self._db_client.store_task_relation(item)
        except (RpcCallError, RuntimeError):
            self._logger.exception("DB RPC failed for %s", type(item).__name__)


def _event_timestamp(event: dict[str, object]) -> datetime:
    raw = event.get("timestamp")
    if isinstance(raw, int | float):
        return datetime.fromtimestamp(float(raw), tz=UTC)
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=UTC)
        return raw
    return datetime.now(UTC)


def _event_received_timestamp(event: dict[str, object]) -> datetime:
    raw = event.get("local_received")
    if isinstance(raw, int | float):
        return datetime.fromtimestamp(float(raw), tz=UTC)
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return raw.replace(tzinfo=UTC)
        return raw
    return _event_timestamp(event)


def _json_default(value: object) -> object:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    return str(value)


def _json_safe(value: dict[str, object]) -> dict[str, object]:
    try:
        safe = json.loads(json.dumps(value, default=_json_default))
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return {"value": str(value)}
    if isinstance(safe, dict):
        return safe
    return {"value": safe}


def _event_field(event: dict[str, object], *keys: str) -> object | None:
    for key in keys:
        value = event.get(key)
        if value is not None:
            return value
    return None


def _event_id(event: dict[str, object], *keys: str) -> object | None:
    value = _event_field(event, *keys)
    if value is not None:
        return value
    headers = event.get("headers")
    if isinstance(headers, dict):
        for key in keys:
            if key in headers:
                return headers[key]
    return None


def _extract_stamps(event: dict[str, object]) -> dict[str, object] | None:
    raw_stamps = event.get("stamps")
    if isinstance(raw_stamps, dict):
        return raw_stamps
    headers = event.get("headers")
    if not isinstance(headers, dict):
        return None
    stamped_headers = headers.get("stamped_headers")
    if isinstance(stamped_headers, list | tuple):
        stamps: dict[str, object] = {}
        for key in stamped_headers:
            if isinstance(key, str) and key in headers:
                stamps[key] = headers[key]
        return stamps or None
    return None


def _parse_iso_datetime(raw: object) -> datetime | None:
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, int | float):
        return datetime.fromtimestamp(float(raw), tz=UTC)
    if isinstance(raw, str):
        value = raw.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _stringify(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, dict | list | tuple | BaseModel):
        try:
            return json.dumps(value, default=_json_default)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float | str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
