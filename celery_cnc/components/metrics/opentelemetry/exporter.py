"""OpenTelemetry exporter for Celery CnC metrics."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlsplit, urlunsplit

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from celery_cnc.components.metrics.base import BaseMonitoringExporter

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from datetime import datetime

    from opentelemetry.metrics import CallbackOptions

    from celery_cnc.core.db.models import TaskEvent, TaskStats, WorkerEvent

__all__ = ["OTelExporter"]


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


class OTelExporter(BaseMonitoringExporter):
    """OpenTelemetry exporter capturing task and worker metrics."""

    def __init__(
        self,
        *,
        service_name: str = "celery-cnc",
        endpoint: str | None = None,
        broker_backend_map: Mapping[str, str] | None = None,
        metric_prefix: str = "celery_cnc",
        metric_reader: MetricReader | None = None,
    ) -> None:
        """Initialize the metrics provider and OTLP exporter."""
        self._endpoint = endpoint
        self._metric_prefix = metric_prefix
        self._broker_backend_map = dict(broker_backend_map or {})
        self._state_lock = threading.Lock()
        self._worker_brokers: dict[str, str] = {}
        self._task_trackers: dict[str, _TaskTracker] = {}
        self._prefetched_counts: dict[tuple[str, str], int] = {}
        self._active_counts: dict[str, int] = {}
        self._worker_online: dict[str, int] = {}
        self._worker_last_heartbeat: dict[str, float] = {}
        self._task_prefetch_times: dict[tuple[str, str], float] = {}
        self._worker_pool_size: dict[str, int] = {}

        resource = Resource.create({"service.name": service_name})
        if metric_reader is None:
            metric_reader = PeriodicExportingMetricReader(_build_otlp_exporter(endpoint))
        self._metric_reader = metric_reader
        self._provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
        self._meter = self._provider.get_meter("celery_cnc.otel")

        self._event_counter = self._meter.create_counter(
            f"{self._metric_prefix}_events_total",
            description="Number of events",
        )
        self._task_failures = self._meter.create_counter(
            f"{self._metric_prefix}_task_failures_total",
            description="Number of failed tasks.",
        )
        self._task_retries = self._meter.create_counter(
            f"{self._metric_prefix}_task_retries_total",
            description="Number of retried tasks.",
        )
        self._task_runtime = self._meter.create_histogram(
            f"{self._metric_prefix}_task_runtime_seconds",
            description="Task runtime",
            unit="s",
            explicit_bucket_boundaries_advisory=_FLOWER_RUNTIME_BUCKETS,
        )
        self._task_runtime_by_task = self._meter.create_histogram(
            f"{self._metric_prefix}_task_runtime_by_task_seconds",
            description="Task runtime aggregated by task name.",
            unit="s",
            explicit_bucket_boundaries_advisory=_FLOWER_RUNTIME_BUCKETS,
        )
        self._task_queue_latency = self._meter.create_histogram(
            f"{self._metric_prefix}_task_queue_latency_seconds",
            description="Time between task received and started.",
            unit="s",
            explicit_bucket_boundaries_advisory=_FLOWER_RUNTIME_BUCKETS,
        )
        self._prefetched_tasks = self._meter.create_up_down_counter(
            f"{self._metric_prefix}_worker_prefetched_tasks",
            description="Number of tasks of given type prefetched at a worker.",
        )
        self._worker_current = self._meter.create_up_down_counter(
            f"{self._metric_prefix}_worker_number_of_currently_executing_tasks",
            description="Number of tasks currently executing at a worker",
        )

        self._task_prefetch_gauge = self._meter.create_observable_gauge(
            f"{self._metric_prefix}_task_prefetch_time_seconds",
            callbacks=[self._observe_task_prefetch],
            unit="s",
            description="The time the task spent waiting at the celery worker to be executed.",
        )
        self._worker_online_gauge = self._meter.create_observable_gauge(
            f"{self._metric_prefix}_worker_online",
            callbacks=[self._observe_worker_online],
            description="Worker online status",
        )
        self._worker_last_heartbeat_gauge = self._meter.create_observable_gauge(
            f"{self._metric_prefix}_worker_last_heartbeat_timestamp_seconds",
            callbacks=[self._observe_worker_last_heartbeat],
            unit="s",
            description="Last worker heartbeat timestamp in seconds since epoch.",
        )
        self._worker_pool_size_gauge = self._meter.create_observable_gauge(
            f"{self._metric_prefix}_worker_pool_size",
            callbacks=[self._observe_worker_pool_size],
            description="Worker pool size",
        )

    @property
    def metric_reader(self) -> MetricReader:
        """Expose the underlying metric reader."""
        return self._metric_reader

    def force_flush(self) -> None:
        """Force a metrics flush for testing."""
        self._provider.force_flush()

    def on_task_event(self, event: TaskEvent) -> None:
        """Update metrics for a task event."""
        worker = event.worker or "unknown"
        task_name = event.name or "unknown"
        state = event.state.upper()
        event_type = _TASK_EVENT_TYPE_BY_STATE.get(state, f"task-{state.lower()}")
        self._event_counter.add(1, attributes=self._task_labels(task_name, worker, event_type))
        if state == "FAILURE":
            self._task_failures.add(1, attributes=self._task_labels(task_name, worker))
        if state == "RETRY":
            self._task_retries.add(1, attributes=self._task_labels(task_name, worker))
        if event.runtime is not None:
            self._task_runtime.record(event.runtime, attributes=self._task_labels(task_name, worker))
            self._task_runtime_by_task.record(
                event.runtime,
                attributes=self._task_summary_labels(task_name, worker),
            )
        self._track_task_state(event, task_name, worker, state)

    def on_worker_event(self, event: WorkerEvent) -> None:
        """Update metrics for a worker event."""
        with self._state_lock:
            if event.broker_url is not None:
                self._worker_brokers[event.hostname] = event.broker_url
            if isinstance(event.info, dict):
                active = _parse_active_tasks(event.info.get("active"))
                if active is not None:
                    self._set_active_count(event.hostname, active)
                pool_size = _parse_pool_size(event.info.get("pool"))
                if pool_size is not None:
                    self._worker_pool_size[event.hostname] = pool_size
            normalized = event.event.lower()
            if normalized in {"worker-online", "online", "worker-heartbeat", "heartbeat"}:
                self._worker_online[event.hostname] = 1
                self._worker_last_heartbeat[event.hostname] = event.timestamp.timestamp()
            elif normalized in {"worker-offline", "offline"}:
                self._worker_online[event.hostname] = 0

    def update_stats(self, stats: TaskStats) -> None:
        """Handle periodic task statistics (noop)."""
        _ = stats

    def serve(self) -> None:
        """Serve exporter data (noop for OTLP exporter)."""
        return

    def shutdown(self) -> None:
        """Shutdown the metrics provider."""
        self._provider.shutdown()

    def _observe_task_prefetch(self, _: CallbackOptions) -> Iterable[Observation]:
        with self._state_lock:
            return [
                Observation(value, attributes=self._task_labels(task, worker))
                for (task, worker), value in self._task_prefetch_times.items()
            ]

    def _observe_worker_online(self, _: CallbackOptions) -> Iterable[Observation]:
        with self._state_lock:
            return [
                Observation(value, attributes=self._worker_labels(worker))
                for worker, value in self._worker_online.items()
            ]

    def _observe_worker_last_heartbeat(self, _: CallbackOptions) -> Iterable[Observation]:
        with self._state_lock:
            return [
                Observation(value, attributes=self._worker_labels(worker))
                for worker, value in self._worker_last_heartbeat.items()
            ]

    def _observe_worker_pool_size(self, _: CallbackOptions) -> Iterable[Observation]:
        with self._state_lock:
            return [
                Observation(value, attributes=self._worker_labels(worker))
                for worker, value in self._worker_pool_size.items()
            ]

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
            with self._state_lock:
                self._task_prefetch_times[(task_name, worker)] = prefetch_seconds
            self._task_queue_latency.record(prefetch_seconds, attributes=self._task_labels(task_name, worker))

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
        previous = self._prefetched_counts.get(key, 0)
        current = previous + delta
        if current <= 0:
            self._prefetched_counts.pop(key, None)
            current = 0
        else:
            self._prefetched_counts[key] = current
        actual_delta = current - previous
        if actual_delta:
            self._prefetched_tasks.add(actual_delta, attributes=self._task_labels(task, worker))

    def _update_active(self, worker: str, delta: int) -> None:
        previous = self._active_counts.get(worker, 0)
        current = previous + delta
        if current <= 0:
            self._active_counts.pop(worker, None)
            current = 0
        else:
            self._active_counts[worker] = current
        actual_delta = current - previous
        if actual_delta:
            self._worker_current.add(actual_delta, attributes=self._worker_labels(worker))

    def _set_active_count(self, worker: str, count: int) -> None:
        previous = self._active_counts.get(worker, 0)
        current = max(count, 0)
        if current <= 0:
            self._active_counts.pop(worker, None)
            current = 0
        else:
            self._active_counts[worker] = current
        actual_delta = current - previous
        if actual_delta:
            self._worker_current.add(actual_delta, attributes=self._worker_labels(worker))

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


def _build_otlp_exporter(endpoint: str | None) -> OTLPMetricExporter:
    if endpoint is None:
        return OTLPMetricExporter()
    parsed = urlsplit(endpoint)
    insecure: bool | None = None
    if parsed.scheme in {"http", "https"}:
        insecure = parsed.scheme == "http"
    return OTLPMetricExporter(endpoint=endpoint, insecure=insecure)


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
