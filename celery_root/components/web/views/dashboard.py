# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Dashboard views and helpers for Celery Root."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, TypedDict

from django.shortcuts import render
from django.utils import timezone

from celery_root.components.web.services import get_registry, open_db
from celery_root.core.db.models import TaskFilter, TimeRange

if TYPE_CHECKING:
    from collections.abc import Sequence

    from django.http import HttpRequest, HttpResponse

    from celery_root.core.db.models import Task, TaskStats, Worker

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
    active: int
    pool_size: int | None
    processed: int
    state_cells: list[_WorkerStateCell]
    last_seen_seconds: int | None
    queues: list[str] | None
    registered: int | None
    concurrency: int | None


class DashboardStats(TypedDict):
    """Serialized dashboard payload for API consumers."""

    generated_at: str
    summary_cards: Sequence[_SummaryCard]
    state_cards: Sequence[_StateCard]
    state_series: Sequence[_StateSeriesPoint]
    throughput_series: Sequence[_ThroughputPoint]
    heatmap: list[list[int]]
    workers: list[_WorkerSummary]
    activity_feed: list[dict[str, object]]
    runtime_stats: dict[str, object]


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
        broker_groups = registry.get_brokers()
        pending_tasks = 0
        for broker_url in broker_groups:
            snapshots = db.get_broker_queue_snapshot(broker_url)
            pending_tasks += sum(event.messages or 0 for event in snapshots)

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
        active = worker.active_tasks if worker.active_tasks is not None else 0
        processed = sum(stats.get(state, 0) for state in ("SUCCESS", "FAILURE", "REVOKED"))
        pool_size = worker.pool_size
        registered = len(worker.registered_tasks or []) or None
        status = _resolve_worker_status(worker.status, active, last_seen)
        badge = _WORKER_BADGES.get(status, "badge-muted")
        rows.append(
            {
                "hostname": worker.hostname,
                "status": status,
                "badge": badge,
                "active": active,
                "pool_size": pool_size,
                "processed": processed,
                "state_cells": state_cells,
                "last_seen_seconds": last_seen_seconds,
                "queues": worker.queues,
                "registered": registered,
                "concurrency": pool_size,
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


def dashboard_stats(now: datetime | None = None) -> DashboardStats:
    """Return dashboard metrics in a structured payload."""
    timestamp = timezone.now() if now is None else now
    metrics = _compute_metrics(timestamp)
    state_cards = _state_cards(timestamp)
    heatmap_range = TimeRange(start=timestamp - timedelta(days=7), end=timestamp)
    with open_db() as db:
        heatmap = db.get_heatmap(heatmap_range)
    activity: list[dict[str, object]] = [
        {
            "task": item["task"],
            "state": item["state"],
            "badge": item["badge"],
            "worker": item["worker"],
            "timestamp": item["timestamp"].isoformat(),
        }
        for item in activity_feed(timestamp)
    ]
    return {
        "generated_at": timestamp.isoformat(),
        "summary_cards": _summary_cards(metrics),
        "state_cards": state_cards,
        "state_series": _state_series(state_cards),
        "throughput_series": _throughput_series(timestamp),
        "heatmap": heatmap,
        "workers": worker_rows(timestamp),
        "activity_feed": activity,
        "runtime_stats": metrics.runtime_stats.model_dump(),
    }


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
