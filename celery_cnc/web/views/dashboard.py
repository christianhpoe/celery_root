"""Dashboard views and helpers for Celery CnC."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypedDict

from django.shortcuts import render
from django.utils import timezone

from celery_cnc.cnc.broker import list_queues
from celery_cnc.db.models import TaskFilter, TimeRange
from celery_cnc.web.services import app_name, get_registry, open_db

if TYPE_CHECKING:
    from collections.abc import Sequence

    from django.http import HttpRequest, HttpResponse

    from celery_cnc.cnc.broker import QueueInfo
    from celery_cnc.db.models import Task, TaskStats, Worker

STATE_BADGES = {
    "SUCCESS": "badge-success",
    "FAILURE": "badge-danger",
    "STARTED": "badge-warning",
    "PENDING": "badge-info",
    "RETRY": "badge-warning",
    "REVOKED": "badge-muted",
}
_OFFLINE_THRESHOLD = timedelta(minutes=2)
_WORKER_BADGES = {
    "online": "badge-success",
    "busy": "badge-warning",
    "offline": "badge-muted",
}
_WORKER_STATE_COLUMNS: tuple[str, ...] = (
    "PENDING",
    "STARTED",
    "SUCCESS",
    "FAILURE",
    "RETRY",
    "REVOKED",
)

WORKER_STATE_COLUMNS: tuple[str, ...] = _WORKER_STATE_COLUMNS


class _SummaryCard(TypedDict):
    title: str
    value: str
    hint: str


class _StateCard(TypedDict):
    label: str
    count: int
    state: str
    change: str


class _ThroughputPoint(TypedDict):
    label: str
    count: int


class _StateSeriesPoint(TypedDict):
    label: str
    count: int


class _ActivityItem(TypedDict):
    task: str
    state: str
    badge: str
    worker: str
    timestamp: datetime


class _WorkerStateCell(TypedDict):
    state: str
    count: int


class _WorkerSummary(TypedDict):
    hostname: str
    status: str
    badge: str
    state_cells: list[_WorkerStateCell]
    last_seen_seconds: int | None


@dataclass(slots=True)
class _SummaryMetrics:
    workers_online: int
    workers_delta: int | None
    tasks_today: int
    tasks_delta_pct: float | None
    runtime_stats: TaskStats
    pending_tasks: int
    workers_under_load: int


def _task_timestamp(task: Task) -> datetime | None:
    return task.finished or task.started or task.received


def _collapse_received(counts: dict[str, int]) -> dict[str, int]:
    if "RECEIVED" not in counts:
        return counts
    collapsed = dict(counts)
    collapsed["PENDING"] = collapsed.get("PENDING", 0) + collapsed.get("RECEIVED", 0)
    collapsed.pop("RECEIVED", None)
    return collapsed


def _format_delta(delta: int | None, *, suffix: str) -> str:
    if delta is None:
        return "no data yet"
    if delta == 0:
        return f"no change {suffix}"
    return f"{delta:+d} {suffix}"


def _format_percent(delta_pct: float | None) -> str:
    if delta_pct is None:
        return "no data yet"
    if delta_pct == 0:
        return "no change"
    arrow = "↗" if delta_pct > 0 else "↘"
    return f"{arrow} {abs(delta_pct):.1f}% throughput"


def _format_runtime(stats: TaskStats) -> tuple[str, str]:
    if stats.avg_runtime is None:
        return "—", "no runtime data"
    p95 = f"{stats.p95:.1f}s" if stats.p95 is not None else "—"
    p99 = f"{stats.p99:.1f}s" if stats.p99 is not None else "—"
    return f"{stats.avg_runtime:.1f}s", f"p95 {p95} · p99 {p99}"


def _worker_online_counts(workers: Sequence[Worker], now: datetime) -> tuple[int, int | None, int]:
    online_threshold = timedelta(minutes=2)
    last_hour_start = now - timedelta(hours=1)
    prev_hour_start = now - timedelta(hours=2)

    online = 0
    count_last_hour = 0
    count_prev_hour = 0
    under_load = 0

    for worker in workers:
        last_seen = worker.last_heartbeat
        if last_seen is None:
            continue
        if now - last_seen <= online_threshold:
            online += 1
        if last_hour_start <= last_seen <= now:
            count_last_hour += 1
        if prev_hour_start <= last_seen < last_hour_start:
            count_prev_hour += 1
        if (worker.active_tasks or 0) > 0:
            under_load += 1

    has_heartbeat = any(worker.last_heartbeat for worker in workers)
    delta = count_last_hour - count_prev_hour if has_heartbeat else None
    return online, delta, under_load


def _resolve_worker_status(status: str, active_tasks: int, last_seen: timedelta | None) -> str:
    if status.upper() == "OFFLINE":
        return "offline"
    if last_seen is not None and last_seen > _OFFLINE_THRESHOLD:
        return "offline"
    if active_tasks > 0:
        return "busy"
    return "online"


def _task_delta_percentage(tasks: Sequence[Task], now: datetime) -> float | None:
    last_hour_start = now - timedelta(hours=1)
    prev_hour_start = now - timedelta(hours=2)
    last_hour_count = 0
    prev_hour_count = 0
    for task in tasks:
        timestamp = _task_timestamp(task)
        if timestamp is None:
            continue
        if last_hour_start <= timestamp <= now:
            last_hour_count += 1
        elif prev_hour_start <= timestamp < last_hour_start:
            prev_hour_count += 1
    if last_hour_count == 0 and prev_hour_count == 0:
        return None
    if prev_hour_count == 0:
        return None
    return (last_hour_count - prev_hour_count) / prev_hour_count * 100


def _compute_metrics(now: datetime) -> _SummaryMetrics:
    with open_db() as db:
        workers = db.get_workers()
        online, delta, under_load = _worker_online_counts(workers, now)

        day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        task_filter = TaskFilter(time_range=TimeRange(start=day_start, end=now))
        tasks_today = db.get_tasks(task_filter)
        stats = db.get_task_stats(None, task_filter.time_range)

        tasks_delta_pct = _task_delta_percentage(tasks_today, now)

        registry = get_registry()
        apps = registry.get_apps()
        queue_infos: list[QueueInfo] = []
        if apps:
            worker_name = app_name(apps[0])
            try:
                queue_infos = list_queues(registry, worker_name)
            except Exception:  # noqa: BLE001  # pragma: no cover - broker failures handled gracefully
                queue_infos = []
        pending_tasks = sum(info.messages or 0 for info in queue_infos)

    return _SummaryMetrics(
        workers_online=online,
        workers_delta=delta,
        tasks_today=len(tasks_today),
        tasks_delta_pct=tasks_delta_pct,
        runtime_stats=stats,
        pending_tasks=pending_tasks,
        workers_under_load=under_load,
    )


def _summary_cards(metrics: _SummaryMetrics) -> Sequence[_SummaryCard]:
    avg_runtime, runtime_hint = _format_runtime(metrics.runtime_stats)
    return [
        {
            "title": "Workers online",
            "value": str(metrics.workers_online),
            "hint": _format_delta(metrics.workers_delta, suffix="vs. last hour"),
        },
        {
            "title": "Tasks today",
            "value": str(metrics.tasks_today),
            "hint": _format_percent(metrics.tasks_delta_pct),
        },
        {
            "title": "Avg. runtime",
            "value": avg_runtime,
            "hint": runtime_hint,
        },
        {
            "title": "Pending queue",
            "value": str(metrics.pending_tasks),
            "hint": f"{metrics.workers_under_load} workers under load",
        },
    ]


def _state_cards(now: datetime) -> Sequence[_StateCard]:
    last_hour_start = now - timedelta(hours=1)
    prev_hour_start = now - timedelta(hours=2)
    last_range = TimeRange(start=last_hour_start, end=now)
    prev_range = TimeRange(start=prev_hour_start, end=last_hour_start)

    with open_db() as db:
        last_tasks = db.get_tasks(TaskFilter(time_range=last_range))
        prev_tasks = db.get_tasks(TaskFilter(time_range=prev_range))
        current_counts = db.get_state_distribution()

    def _count_by_state(tasks: Sequence[Task]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for task in tasks:
            counts[task.state] = counts.get(task.state, 0) + 1
        return counts

    last_counts = _count_by_state(last_tasks)
    prev_counts = _count_by_state(prev_tasks)
    current_counts = _collapse_received(current_counts)
    last_counts = _collapse_received(last_counts)
    prev_counts = _collapse_received(prev_counts)

    def _delta(state: str) -> str:
        diff = last_counts.get(state, 0) - prev_counts.get(state, 0)
        if diff == 0:
            return "0"
        arrow = "↗" if diff > 0 else "↘"
        return f"{arrow} {abs(diff)}"

    label_map = {
        "SUCCESS": "Succeeded",
        "FAILURE": "Failed",
        "STARTED": "Running",
        "PENDING": "Pending",
    }
    ordered_states = ["SUCCESS", "FAILURE", "STARTED", "PENDING"]
    return [
        {
            "label": label_map[state],
            "count": current_counts.get(state, 0),
            "state": state,
            "change": _delta(state),
        }
        for state in ordered_states
    ]


def _throughput_series(now: datetime) -> Sequence[_ThroughputPoint]:
    time_range = TimeRange(start=now - timedelta(hours=1), end=now)
    with open_db() as db:
        buckets = db.get_throughput(time_range, bucket_seconds=600)
    return [{"label": bucket.bucket_start.strftime("%H:%M"), "count": bucket.count} for bucket in buckets]


def _state_series(cards: Sequence[_StateCard]) -> Sequence[_StateSeriesPoint]:
    return [{"label": card["label"], "count": card["count"]} for card in cards]


def throughput_series(now: datetime) -> Sequence[_ThroughputPoint]:
    """Expose throughput series for API consumers."""
    return _throughput_series(now)


def state_cards(now: datetime) -> Sequence[_StateCard]:
    """Expose state cards for API consumers."""
    return _state_cards(now)


def state_series(cards: Sequence[_StateCard]) -> Sequence[_StateSeriesPoint]:
    """Expose state series for API consumers."""
    return _state_series(cards)


def _activity_feed(now: datetime) -> Sequence[_ActivityItem]:
    time_range = TimeRange(start=now - timedelta(hours=24), end=now)
    with open_db() as db:
        tasks = db.get_tasks(TaskFilter(time_range=time_range))
    items: list[_ActivityItem] = []
    for task in tasks[:5]:
        timestamp = _task_timestamp(task)
        if timestamp is None:
            continue
        items.append(
            {
                "task": task.name or task.task_id,
                "state": task.state,
                "badge": STATE_BADGES.get(task.state, "badge-muted"),
                "worker": task.worker or "—",
                "timestamp": timestamp,
            },
        )
    return items


def _worker_summary(now: datetime) -> list[_WorkerSummary]:
    with open_db() as db:
        workers = db.get_workers()
        tasks = db.get_tasks()

    counts: dict[str, dict[str, int]] = {}
    for worker in workers:
        counts[worker.hostname] = dict.fromkeys(_WORKER_STATE_COLUMNS, 0)

    for task in tasks:
        if task.worker is None:
            continue
        if task.worker not in counts:
            continue
        state = "PENDING" if task.state == "RECEIVED" else task.state
        if state in counts[task.worker]:
            counts[task.worker][state] += 1

    rows: list[_WorkerSummary] = []
    for worker in workers:
        last_seen_seconds = None
        if worker.last_heartbeat is not None:
            last_seen = now - worker.last_heartbeat
            last_seen_seconds = int(last_seen.total_seconds())
        else:
            last_seen = None
        stats = counts.get(worker.hostname, dict.fromkeys(_WORKER_STATE_COLUMNS, 0))
        state_cells: list[_WorkerStateCell] = [
            {"state": state, "count": stats.get(state, 0)} for state in _WORKER_STATE_COLUMNS
        ]
        active = stats.get("STARTED", 0) if worker.active_tasks is None else worker.active_tasks
        status = _resolve_worker_status(worker.status, active, last_seen)
        badge = _WORKER_BADGES.get(status, "badge-muted")
        rows.append(
            {
                "hostname": worker.hostname,
                "status": status,
                "badge": badge,
                "state_cells": state_cells,
                "last_seen_seconds": last_seen_seconds,
            },
        )
    rows.sort(key=lambda row: row["hostname"])
    return rows


def activity_feed(now: datetime) -> Sequence[_ActivityItem]:
    """Return recent activity entries for API consumers."""
    return _activity_feed(now)


def worker_rows(now: datetime) -> list[_WorkerSummary]:
    """Expose worker summary rows for templates and API."""
    return _worker_summary(now)


def dashboard(request: HttpRequest) -> HttpResponse:
    """Render the dashboard page."""
    context = {
        "title": "Dashboard",
    }
    return render(request, "dashboard.html", context)


def dashboard_fragment(request: HttpRequest) -> HttpResponse:
    """Render the dashboard fragment."""
    now = timezone.now()
    metrics = _compute_metrics(now)
    state_cards = _state_cards(now)
    context = {
        "summary_cards": _summary_cards(metrics),
        "state_cards": state_cards,
        "worker_rows": worker_rows(now),
        "worker_state_columns": _WORKER_STATE_COLUMNS,
    }
    return render(request, "dashboard_content.html", context)
