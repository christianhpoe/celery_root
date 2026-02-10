"""Celery event listener process."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from multiprocessing import Event, Process, Queue
from typing import TYPE_CHECKING

from celery import Celery
from celery.events import EventReceiver
from kombu.exceptions import OperationalError

from celery_cnc.config import set_settings
from celery_cnc.core.db.models import TaskEvent, TaskRelation, WorkerEvent
from celery_cnc.core.logging.setup import configure_process_logging
from celery_cnc.core.logging.utils import sanitize_component

if TYPE_CHECKING:
    from celery_cnc.config import CeleryCnCConfig

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


class EventListener(Process):
    """Listen to Celery events for a single broker."""

    def __init__(self, broker_url: str, queue: Queue[object], config: CeleryCnCConfig | None = None) -> None:
        """Create an event listener for a broker URL."""
        super().__init__(daemon=True)
        self.broker_url = broker_url
        self.queue = queue
        self._config = config
        self._stop_event = Event()
        self._logger = logging.getLogger(__name__)

    def stop(self) -> None:
        """Signal the listener to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Listen for events and forward them onto the queue."""
        component = f"event_listener-{sanitize_component(self.broker_url)}"
        if self._config is not None:
            set_settings(self._config)
            configure_process_logging(self._config, component=component)
        else:
            configure_process_logging(component=component)
        self._logger.info("EventListener starting for %s", self.broker_url)
        app = Celery(broker=self.broker_url)
        last_heartbeat = time.monotonic()
        while not self._stop_event.is_set():
            try:
                last_heartbeat = self._listen(app, last_heartbeat)
            except (OperationalError, OSError) as exc:
                self._logger.warning("EventListener reconnecting to %s: %s", self.broker_url, exc)
                time.sleep(1.0)
            except KeyboardInterrupt:
                self._logger.info("EventListener interrupted for %s; stopping.", self.broker_url)
                self._stop_event.set()
            except Exception:  # pragma: no cover - defensive
                self._logger.exception("EventListener error for %s", self.broker_url)
                time.sleep(1.0)
        self._logger.info("EventListener stopped for %s", self.broker_url)

    def _listen(self, app: Celery, last_heartbeat: float) -> float:
        with app.connection() as connection:
            self._logger.info("EventListener connected to %s", self.broker_url)
            last_enable = 0.0
            receiver = EventReceiver(
                connection,
                handlers={"*": self._handle_event},
                app=app,
            )
            try:
                while not self._stop_event.is_set():
                    now = time.monotonic()
                    last_heartbeat = self._maybe_log_heartbeat(now, last_heartbeat)
                    last_enable = self._maybe_enable_events(app, last_enable)
                    if not self._capture_once(receiver):
                        break
            finally:
                self._logger.info("EventListener disconnected from %s", self.broker_url)
        return last_heartbeat

    def _maybe_log_heartbeat(self, now: float, last_heartbeat: float) -> float:
        if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
            self._logger.info("EventListener heartbeat for %s", self.broker_url)
            return now
        return last_heartbeat

    def _maybe_enable_events(self, app: Celery, last_enable: float) -> float:
        if time.monotonic() - last_enable < _ENABLE_EVENTS_INTERVAL:
            return last_enable
        try:
            app.control.enable_events()
        except Exception:  # pragma: no cover - best effort to enable events  # noqa: BLE001
            self._logger.debug("Failed to enable worker events for %s", self.broker_url)
            return last_enable
        else:
            now = time.monotonic()
            self._logger.info("Enabled events for workers on %s", self.broker_url)
            return now

    def _capture_once(self, receiver: EventReceiver) -> bool:
        try:
            receiver.capture(limit=None, timeout=1.0, wakeup=True)
        except TimeoutError:
            return True
        except KeyboardInterrupt:
            self._stop_event.set()
            return False
        return True

    def _handle_event(self, event: dict[str, object]) -> None:
        event_type = str(event.get("type", ""))
        if event_type:
            self._logger.debug("Event received from %s: %s", self.broker_url, event_type)
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
            kwargs=_stringify(kwargs),
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
        self.queue.put(task_event)

    def _handle_worker_event(self, event_type: str, event: dict[str, object]) -> None:
        hostname = event.get("hostname")
        if not hostname:
            return
        if event_type in {"worker-online", "worker-offline"}:
            self._logger.info("Worker %s event from %s: %s", hostname, self.broker_url, event_type)
        info = {key: value for key, value in event.items() if key not in {"type", "timestamp"}}
        worker_event = WorkerEvent(
            hostname=str(hostname),
            event=event_type,
            timestamp=_event_received_timestamp(event),
            info=info,
            broker_url=self.broker_url,
        )
        self.queue.put(worker_event)

    def _handle_task_relation(self, event: dict[str, object]) -> None:
        root_id = _stringify(event.get("root_id"))
        child_id = _stringify(event.get("child_id"))
        relation = _stringify(event.get("relation"))
        if not root_id or not child_id or not relation:
            return
        parent_id = _stringify(event.get("parent_id"))
        self.queue.put(
            TaskRelation(
                root_id=root_id,
                parent_id=parent_id,
                child_id=child_id,
                relation=relation,
            ),
        )


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
    if isinstance(value, dict | list | tuple):
        try:
            return json.dumps(value)
        except TypeError:
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
