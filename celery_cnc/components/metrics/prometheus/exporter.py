"""Prometheus exporter for Celery CnC metrics."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

from prometheus_client import (
    GC_COLLECTOR,
    PLATFORM_COLLECTOR,
    PROCESS_COLLECTOR,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from celery_cnc.components.metrics.base import BaseMonitoringExporter

if TYPE_CHECKING:
    from collections.abc import Mapping
    from datetime import datetime

    from celery_cnc.core.db.models import TaskEvent, TaskStats, WorkerEvent

__all__ = ["PrometheusExporter"]


_TASK_EVENT_TYPE_BY_STATE = {
    "RECEIVED": "task-received",
    "STARTED": "task-started",
    "SUCCESS": "task-succeeded",
    "FAILURE": "task-failed",
    "RETRY": "task-retried",
    "REVOKED": "task-revoked",
    "REJECTED": "task-rejected",
    "PENDING": "task-sent",
}
_TERMINAL_TASK_STATES = {"SUCCESS", "FAILURE", "REVOKED", "REJECTED"}
_FLOWER_RUNTIME_BUCKETS = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
)


@dataclass(slots=True)
class _TaskTracker:
    name: str
    worker: str
    state: str
    received_at: datetime | None


class PrometheusExporter(BaseMonitoringExporter):
    """Prometheus exporter capturing task and worker metrics."""

    def __init__(
        self,
        *,
        port: int | None = None,
        registry: CollectorRegistry | None = None,
        broker_backend_map: Mapping[str, str] | None = None,
        flower_compatibility: bool = False,
    ) -> None:
        """Initialize metrics and optionally start the HTTP server."""
        self.registry = registry or CollectorRegistry()
        self._register_default_collectors()
        self._broker_backend_map = dict(broker_backend_map or {})
        self._worker_brokers: dict[str, str] = {}
        self._task_trackers: dict[str, _TaskTracker] = {}
        self._prefetched_counts: dict[tuple[str, str], int] = {}
        self._active_counts: dict[str, int] = {}
        self._metric_prefix = "flower" if flower_compatibility else "celery_cnc"

        self._event_counter = Counter(
            f"{self._metric_prefix}_events",
            "Number of events",
            ("task", "type", "worker", "broker", "backend"),
            registry=self.registry,
        )
        self._task_failures = Counter(
            f"{self._metric_prefix}_task_failures_total",
            "Number of failed tasks.",
            ("task", "worker", "broker", "backend"),
            registry=self.registry,
        )
        self._task_retries = Counter(
            f"{self._metric_prefix}_task_retries_total",
            "Number of retried tasks.",
            ("task", "worker", "broker", "backend"),
            registry=self.registry,
        )
        self._task_runtime = Histogram(
            f"{self._metric_prefix}_task_runtime_seconds",
            "Task runtime",
            ("task", "worker", "broker", "backend"),
            registry=self.registry,
            buckets=_FLOWER_RUNTIME_BUCKETS,
        )
        self._task_runtime_by_task = Histogram(
            f"{self._metric_prefix}_task_runtime_by_task_seconds",
            "Task runtime aggregated by task name.",
            ("task", "broker", "backend"),
            registry=self.registry,
            buckets=_FLOWER_RUNTIME_BUCKETS,
        )
        self._task_prefetch = Gauge(
            f"{self._metric_prefix}_task_prefetch_time_seconds",
            "The time the task spent waiting at the celery worker to be executed.",
            ("task", "worker", "broker", "backend"),
            registry=self.registry,
        )
        self._task_queue_latency = Histogram(
            f"{self._metric_prefix}_task_queue_latency_seconds",
            "Time between task received and started.",
            ("task", "worker", "broker", "backend"),
            registry=self.registry,
            buckets=_FLOWER_RUNTIME_BUCKETS,
        )
        self._prefetched_tasks = Gauge(
            f"{self._metric_prefix}_worker_prefetched_tasks",
            "Number of tasks of given type prefetched at a worker.",
            ("task", "worker", "broker", "backend"),
            registry=self.registry,
        )
        self._worker_online = Gauge(
            f"{self._metric_prefix}_worker_online",
            "Worker online status",
            ("worker", "broker", "backend"),
            registry=self.registry,
        )
        self._worker_last_heartbeat = Gauge(
            f"{self._metric_prefix}_worker_last_heartbeat_timestamp_seconds",
            "Last worker heartbeat timestamp in seconds since epoch.",
            ("worker", "broker", "backend"),
            registry=self.registry,
        )
        self._worker_current = Gauge(
            f"{self._metric_prefix}_worker_number_of_currently_executing_tasks",
            "Number of tasks currently executing at a worker",
            ("worker", "broker", "backend"),
            registry=self.registry,
        )
        self._worker_pool_size = Gauge(
            f"{self._metric_prefix}_worker_pool_size",
            "Worker pool size",
            ("worker", "broker", "backend"),
            registry=self.registry,
        )

        self._started_server = False
        if port is not None:
            start_http_server(port, registry=self.registry)
            self._started_server = True

    def on_task_event(self, event: TaskEvent) -> None:
        """Update metrics for a task event."""
        worker = event.worker or "unknown"
        task_name = event.name or "unknown"
        state = event.state.upper()
        event_type = _TASK_EVENT_TYPE_BY_STATE.get(state, f"task-{state.lower()}")
        self._event_counter.labels(**self._task_labels(task_name, worker, event_type)).inc()
        if state == "FAILURE":
            self._task_failures.labels(**self._task_labels(task_name, worker)).inc()
        if state == "RETRY":
            self._task_retries.labels(**self._task_labels(task_name, worker)).inc()
        if event.runtime is not None:
            self._task_runtime.labels(**self._task_labels(task_name, worker)).observe(event.runtime)
            self._task_runtime_by_task.labels(**self._task_summary_labels(task_name, worker)).observe(
                event.runtime,
            )
        self._track_task_state(event, task_name, worker, state)

    def on_worker_event(self, event: WorkerEvent) -> None:
        """Update metrics for a worker event."""
        if event.broker_url is not None:
            self._worker_brokers[event.hostname] = event.broker_url
        labels = self._worker_labels(event.hostname)
        normalized = event.event.lower()
        if isinstance(event.info, dict):
            active = _parse_active_tasks(event.info.get("active"))
            if active is not None:
                self._set_active_count(event.hostname, active)
            pool_size = _parse_pool_size(event.info.get("pool"))
            if pool_size is not None:
                self._worker_pool_size.labels(**labels).set(pool_size)
        if normalized in {"worker-online", "online", "worker-heartbeat", "heartbeat"}:
            self._worker_online.labels(**labels).set(1)
            self._worker_last_heartbeat.labels(**labels).set(event.timestamp.timestamp())
        elif normalized in {"worker-offline", "offline"}:
            self._worker_online.labels(**labels).set(0)

    def update_stats(self, stats: TaskStats) -> None:
        """Update runtime gauges from task statistics."""
        _ = stats

    def serve(self) -> None:
        """Start the HTTP server if not already running."""
        if not self._started_server:
            start_http_server(8001, registry=self.registry)
            self._started_server = True

    def shutdown(self) -> None:
        """Shutdown hook for API parity (noop)."""
        return

    def _register_default_collectors(self) -> None:
        for collector in (PROCESS_COLLECTOR, PLATFORM_COLLECTOR, GC_COLLECTOR):
            try:
                self.registry.register(collector)
            except ValueError:
                continue

    def _track_task_state(self, event: TaskEvent, task_name: str, worker: str, state: str) -> None:
        task_id = event.task_id
        previous = self._task_trackers.get(task_id)
        prev_state = previous.state if previous else None
        prev_name = previous.name if previous else task_name
        prev_worker = previous.worker if previous else worker

        if prev_state == "RECEIVED" and state != "RECEIVED":
            self._update_prefetched(prev_name, prev_worker, -1)
        if prev_state == "STARTED" and state != "STARTED":
            self._update_active(prev_worker, -1)

        if state == "RECEIVED" and prev_state != "RECEIVED":
            self._update_prefetched(task_name, worker, 1)
        if state == "STARTED" and prev_state != "STARTED":
            self._update_active(worker, 1)

        if prev_state == "RECEIVED" and state == "STARTED" and previous and previous.received_at:
            prefetch_seconds = (event.timestamp - previous.received_at).total_seconds()
            if prefetch_seconds < 0:
                prefetch_seconds = 0.0
            self._task_prefetch.labels(**self._task_labels(task_name, worker)).set(prefetch_seconds)
            self._task_queue_latency.labels(**self._task_labels(task_name, worker)).observe(prefetch_seconds)

        if state in _TERMINAL_TASK_STATES:
            self._task_trackers.pop(task_id, None)
            return

        received_at = event.timestamp if state == "RECEIVED" else None
        self._task_trackers[task_id] = _TaskTracker(
            name=task_name,
            worker=worker,
            state=state,
            received_at=received_at,
        )

    def _update_prefetched(self, task: str, worker: str, delta: int) -> None:
        key = (task, worker)
        current = self._prefetched_counts.get(key, 0) + delta
        if current <= 0:
            self._prefetched_counts.pop(key, None)
            current = 0
        else:
            self._prefetched_counts[key] = current
        self._prefetched_tasks.labels(**self._task_labels(task, worker)).set(current)

    def _update_active(self, worker: str, delta: int) -> None:
        current = self._active_counts.get(worker, 0) + delta
        if current <= 0:
            self._active_counts.pop(worker, None)
            current = 0
        else:
            self._active_counts[worker] = current
        self._worker_current.labels(**self._worker_labels(worker)).set(current)

    def _set_active_count(self, worker: str, count: int) -> None:
        current = max(count, 0)
        if current <= 0:
            self._active_counts.pop(worker, None)
            current = 0
        else:
            self._active_counts[worker] = current
        self._worker_current.labels(**self._worker_labels(worker)).set(current)

    def _task_labels(self, task: str, worker: str, event_type: str | None = None) -> dict[str, str]:
        labels = {
            "task": task,
            "worker": worker,
            "broker": self._broker_label(worker),
            "backend": self._backend_label(worker),
        }
        if event_type is not None:
            labels["type"] = event_type
        return labels

    def _task_summary_labels(self, task: str, worker: str) -> dict[str, str]:
        return {
            "task": task,
            "broker": self._broker_label(worker),
            "backend": self._backend_label(worker),
        }

    def _worker_labels(self, worker: str) -> dict[str, str]:
        return {
            "worker": worker,
            "broker": self._broker_label(worker),
            "backend": self._backend_label(worker),
        }

    def _broker_label(self, worker: str) -> str:
        broker_url = self._worker_brokers.get(worker)
        return _normalize_label(broker_url, empty_label="default")

    def _backend_label(self, worker: str) -> str:
        broker_url = self._worker_brokers.get(worker)
        backend = self._broker_backend_map.get(broker_url or "") if broker_url is not None else None
        return _normalize_label(backend, empty_label="disabled")


def _normalize_label(value: str | None, *, empty_label: str) -> str:
    if value is None:
        return "unknown"
    if value == "":
        return empty_label
    return _strip_credentials(value)


def _strip_credentials(value: str) -> str:
    try:
        parts = urlsplit(value)
    except ValueError:
        return value
    if not parts.scheme or not parts.netloc:
        return value
    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    return urlunsplit((parts.scheme, host, parts.path, parts.query, parts.fragment))


def _parse_pool_size(value: object) -> int | None:
    if not isinstance(value, dict):
        return None
    pool_size = value.get("max-concurrency") or value.get("max_concurrency")
    if isinstance(pool_size, int):
        return pool_size
    return None


def _parse_active_tasks(value: object) -> int | None:
    if isinstance(value, list):
        return len(value)
    return None
