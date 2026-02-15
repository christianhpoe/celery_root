# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Background reconciler for worker/broker stats and task completion."""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from multiprocessing import Event, Process
from typing import TYPE_CHECKING

from celery_root.config import set_settings
from celery_root.core.db.models import BrokerQueueEvent, Task, TaskEvent, TaskFilter, WorkerEvent
from celery_root.core.db.rpc_client import DbRpcClient, RpcCallError
from celery_root.core.engine.brokers import list_queues
from celery_root.core.logging.setup import configure_process_logging
from celery_root.core.registry import WorkerRegistry
from celery_root.shared.redaction import redact_access_data, redact_url_password

if TYPE_CHECKING:
    from celery import Celery

    from celery_root.config import CeleryRootConfig

_FINAL_STATES = {"SUCCESS", "FAILURE", "REVOKED"}
_NON_FINAL_STATES = ("PENDING", "RECEIVED", "STARTED", "RETRY", "REJECTED")

_LOOP_INTERVAL_SECONDS = 1.0
_WORKER_REFRESH_SECONDS = 30.0
_TASK_REFRESH_SECONDS = 30.0

_WorkerSnapshot = tuple[
    Mapping[str, object] | None,
    Mapping[str, object] | None,
    object | None,
    object | None,
    object | None,
    str | None,
    str | None,
]


def _app_name(app: Celery) -> str:
    raw_main = getattr(app, "main", None)
    name = str(raw_main) if raw_main else ""
    if not name:
        conf_main = app.conf.get("main")
        name = str(conf_main) if conf_main else ""
    if not name:
        name = f"celery_app_{id(app)}"
    return name


def _safe_call(call: Callable[[], object]) -> object | None:
    try:
        return call()
    except Exception:  # noqa: BLE001 - best-effort inspector calls
        return None


def _extract_host_payload(payload: object, hostname: str) -> object | None:
    if isinstance(payload, dict):
        return payload.get(hostname)
    return None


def _parse_datetime(raw: object) -> datetime | None:
    if raw is None:
        return None
    parsed: datetime | None = None
    if isinstance(raw, datetime):
        parsed = raw if raw.tzinfo is not None else raw.replace(tzinfo=UTC)
    elif isinstance(raw, int | float):
        parsed = datetime.fromtimestamp(float(raw), tz=UTC)
    elif isinstance(raw, str):
        value = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            parsed = None
        else:
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _json_safe(value: dict[str, object]) -> dict[str, object]:
    try:
        safe = json.loads(json.dumps(value, default=str))
    except (TypeError, ValueError):
        return {"value": str(value)}
    if isinstance(safe, dict):
        return safe
    return {"value": safe}


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


def _final_state(meta: Mapping[str, object]) -> str | None:
    status = meta.get("status")
    if status is None:
        return None
    status_str = str(status)
    if status_str in _FINAL_STATES:
        return status_str
    return None


def _task_event_from_meta(task: Task, status: str, meta: Mapping[str, object]) -> TaskEvent:
    timestamp = _parse_datetime(meta.get("date_done")) or datetime.now(UTC)
    return TaskEvent(
        task_id=task.task_id,
        name=task.name,
        state=status,
        timestamp=timestamp,
        worker=task.worker,
        result=_stringify(meta.get("result")),
        traceback=_stringify(meta.get("traceback")),
        runtime=_to_float(meta.get("runtime")),
        retries=_to_int(meta.get("retries")),
        parent_id=task.parent_id,
        root_id=task.root_id,
        group_id=task.group_id,
        chord_id=task.chord_id,
    )


class Reconciler(Process):
    """Background reconciler for periodic inspections and task completion backfill."""

    def __init__(self, config: CeleryRootConfig) -> None:
        """Initialize the reconciler process."""
        super().__init__(daemon=True)
        self._config = config
        self._stop_event = Event()
        self._logger = logging.getLogger(__name__)
        self._db_client: DbRpcClient | None = None
        self._apps: tuple[Celery, ...] = ()
        self._app_names: list[str] = []
        self._worker_names: list[str] = []
        self._worker_index = 0
        self._broker_index = 0
        self._task_queue: deque[Task] = deque()
        self._task_refresh_at = 0.0
        self._worker_refresh_at = 0.0
        self._round_robin_index = 0

    def stop(self) -> None:
        """Signal the reconciler to stop."""
        self._stop_event.set()

    def run(self) -> None:
        """Run reconciliation loop until stopped."""
        set_settings(self._config)
        configure_process_logging(self._config, component="reconciler")
        self._logger.info("Reconciler starting.")
        if self._config is not None:
            self._logger.info("Reconciler DB RPC socket path: %s", self._config.database.rpc_socket_path)
            self._logger.info("Reconciler DB RPC auth enabled: %s", bool(self._config.database.rpc_auth_key))

        self._db_client = DbRpcClient.from_config(self._config, client_name="reconciler")
        registry = WorkerRegistry(self._config.worker_import_paths)
        self._apps = registry.get_apps()
        self._app_names = [_app_name(app) for app in self._apps]

        actions = ("workers", "brokers", "tasks")
        while not self._stop_event.is_set():
            action = actions[self._round_robin_index]
            self._round_robin_index = (self._round_robin_index + 1) % len(actions)
            try:
                if action == "workers":
                    self._poll_worker_stats()
                elif action == "brokers":
                    self._poll_broker_stats(registry)
                else:
                    self._reconcile_task_states()
            except Exception:  # pragma: no cover - defensive
                self._logger.exception("Reconciler loop error.")
            time.sleep(_LOOP_INTERVAL_SECONDS)

        if self._db_client is not None:
            self._db_client.close()
        self._logger.info("Reconciler stopped.")

    def _poll_worker_stats(self) -> None:
        if not self._apps:
            return
        now = time.monotonic()
        if self._should_refresh_workers(now):
            self._refresh_workers()
        hostname = self._next_worker_name()
        if hostname is None:
            return
        snapshot = self._inspect_worker(hostname)
        event = self._build_worker_event(hostname, snapshot)
        if event is None:
            return
        self._store_worker_event(event, hostname)

    def _should_refresh_workers(self, now: float) -> bool:
        return now - self._worker_refresh_at >= _WORKER_REFRESH_SECONDS or not self._worker_names

    def _next_worker_name(self) -> str | None:
        if not self._worker_names:
            return None
        hostname = self._worker_names[self._worker_index % len(self._worker_names)]
        self._worker_index += 1
        return hostname

    def _build_worker_event(
        self,
        hostname: str,
        snapshot: _WorkerSnapshot,
    ) -> WorkerEvent | None:
        stats, conf, queues, registered, active, broker_url, _app_name = snapshot
        if stats is None and conf is None and queues is None and registered is None and active is None:
            return None
        info = self._build_worker_info(snapshot)
        redacted_broker_url = redact_url_password(broker_url) if broker_url is not None else None
        return WorkerEvent(
            hostname=hostname,
            event="worker-snapshot",
            timestamp=datetime.now(UTC),
            info=info,
            broker_url=redacted_broker_url,
        )

    def _build_worker_info(
        self,
        snapshot: _WorkerSnapshot,
    ) -> dict[str, object]:
        stats, conf, queues, registered, active, _broker_url, app_name = snapshot
        info: dict[str, object] = {}
        if stats:
            info["stats"] = stats
            pool = stats.get("pool")
            if pool is not None:
                info["pool"] = pool
        if conf:
            info["conf"] = conf
        if queues is not None:
            info["queues"] = queues
        if registered is not None:
            info["registered"] = registered
        active_value = self._active_count(active)
        if active_value is not None:
            info["active"] = active_value
        elif active is not None:
            info["active"] = active
        if app_name is not None:
            info["app"] = app_name
        redacted = redact_access_data(info)
        if isinstance(redacted, dict):
            return _json_safe(redacted)
        return _json_safe({"value": redacted})

    def _active_count(self, active: object) -> int | None:
        if isinstance(active, Sequence) and not isinstance(active, str | bytes):
            return len(active)
        if isinstance(active, int):
            return active
        return None

    def _store_worker_event(self, event: WorkerEvent, hostname: str) -> None:
        if self._db_client is None:
            return
        active_value = event.info.get("active") if isinstance(event.info, Mapping) else None
        try:
            self._db_client.store_worker_event(event)
        except (RpcCallError, RuntimeError):
            self._logger.exception("Reconciler failed to store worker stats %s via RPC", hostname)
            return
        self._logger.debug(
            "Reconciler worker snapshot persisted hostname=%s active=%s",
            hostname,
            active_value,
        )

    def _inspect_worker(
        self,
        hostname: str,
    ) -> tuple[
        Mapping[str, object] | None,
        Mapping[str, object] | None,
        object | None,
        object | None,
        object | None,
        str | None,
        str | None,
    ]:
        for app in self._apps:
            broker_url = str(app.conf.broker_url or "")
            app_name = _app_name(app)
            try:
                inspector = app.control.inspect(timeout=1.0, destination=[hostname])
            except TypeError:
                inspector = app.control.inspect(destination=[hostname])
            if inspector is None:
                continue
            stats_payload = _safe_call(inspector.stats)
            conf_payload = _safe_call(inspector.conf)
            queues_payload = _safe_call(inspector.active_queues)
            active_payload = _safe_call(inspector.active)
            registered_payload = _safe_call(inspector.registered)
            stats = _extract_host_payload(stats_payload, hostname)
            conf = _extract_host_payload(conf_payload, hostname)
            queues = _extract_host_payload(queues_payload, hostname)
            active = _extract_host_payload(active_payload, hostname)
            registered = _extract_host_payload(registered_payload, hostname)
            if (
                stats is not None
                or conf is not None
                or queues is not None
                or registered is not None
                or active is not None
            ):
                stats_map = stats if isinstance(stats, Mapping) else None
                conf_map = conf if isinstance(conf, Mapping) else None
                return stats_map, conf_map, queues, registered, active, broker_url, app_name
        return None, None, None, None, None, None, None

    def _refresh_workers(self) -> None:
        if self._db_client is None:
            return
        try:
            workers = self._db_client.get_workers()
        except (RpcCallError, RuntimeError):
            self._logger.exception("Reconciler failed to fetch workers via RPC")
            return
        self._worker_names = [worker.hostname for worker in workers]
        self._worker_refresh_at = time.monotonic()
        self._worker_index = 0

    def _poll_broker_stats(self, registry: WorkerRegistry) -> None:
        if not self._app_names:
            return
        app_name = self._app_names[self._broker_index % len(self._app_names)]
        self._broker_index += 1
        broker_url = ""
        try:
            app = registry.get_app(app_name)
        except KeyError:
            app = None
        if app is not None:
            broker_url = str(app.conf.broker_url or "")
        redacted_broker_url = redact_url_password(broker_url) if broker_url else broker_url
        try:
            queues = list_queues(registry, app_name)
        except Exception:  # pragma: no cover - broker dependent
            self._logger.exception("Reconciler failed to fetch broker stats for %s", app_name)
            return
        pending = sum(queue.messages or 0 for queue in queues)
        self._logger.debug("Reconciler broker stats app=%s queues=%d pending=%d", app_name, len(queues), pending)
        if self._db_client is None:
            return
        timestamp = datetime.now(UTC)
        for queue in queues:
            event = BrokerQueueEvent(
                broker_url=redacted_broker_url,
                queue=queue.name,
                messages=queue.messages,
                consumers=queue.consumers,
                timestamp=timestamp,
            )
            try:
                self._db_client.store_broker_queue_event(event)
            except (RpcCallError, RuntimeError):
                self._logger.exception("Reconciler failed to store broker queue %s via RPC", queue.name)

    def _reconcile_task_states(self) -> None:
        if self._db_client is None or not self._apps:
            return
        now = time.monotonic()
        if now - self._task_refresh_at >= _TASK_REFRESH_SECONDS or not self._task_queue:
            self._refresh_tasks()
        if not self._task_queue:
            return
        task = self._task_queue.popleft()
        meta, status = self._find_final_meta(task)
        if meta is None or status is None:
            return
        event = _task_event_from_meta(task, status, meta)
        try:
            self._db_client.store_task_event(event)
        except (RpcCallError, RuntimeError):
            self._logger.exception("Reconciler failed to store task event %s via RPC", task.task_id)
            return
        self._logger.info("Reconciler backfilled task %s state=%s", task.task_id, status)

    def _refresh_tasks(self) -> None:
        if self._db_client is None:
            return
        tasks: dict[str, Task] = {}
        for state in _NON_FINAL_STATES:
            try:
                batch = self._db_client.get_tasks(TaskFilter(state=state))
            except (RpcCallError, RuntimeError):
                self._logger.exception("Reconciler failed to fetch tasks state=%s via RPC", state)
                continue
            for task in batch:
                tasks[task.task_id] = task
        self._task_queue = deque(tasks.values())
        self._task_refresh_at = time.monotonic()

    def _find_final_meta(self, task: Task) -> tuple[Mapping[str, object] | None, str | None]:
        for app in self._apps:
            backend = getattr(app, "backend", None)
            get_task_meta = getattr(backend, "get_task_meta", None)
            if not callable(get_task_meta):
                continue
            try:
                meta = get_task_meta(task.task_id)
            except Exception:  # pragma: no cover - backend dependent
                self._logger.exception("Reconciler backend error app=%s task_id=%s", _app_name(app), task.task_id)
                continue
            if not isinstance(meta, Mapping):
                continue
            status = _final_state(meta)
            if status is not None:
                return meta, status
        return None, None
