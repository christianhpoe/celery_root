# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Worker list and detail views."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, TypedDict

from django.http import Http404, HttpResponseBadRequest
from django.shortcuts import redirect, render
from django.utils import timezone

from celery_root.components.web.services import app_name, get_registry, list_worker_options, open_db
from celery_root.components.web.views import broker as broker_views
from celery_root.core.db.models import TaskFilter
from celery_root.core.engine import workers as worker_control

from .decorators import require_post

if TYPE_CHECKING:
    from datetime import datetime

    from django.http import HttpRequest, HttpResponse

_WORKER_NOT_FOUND = "Worker not found"
_OFFLINE_THRESHOLD = timedelta(minutes=2)

STATUS_BADGES = {
    "online": "badge-success",
    "busy": "badge-warning",
    "offline": "badge-muted",
}

TASK_BADGES = {
    "SUCCESS": "badge-success",
    "FAILURE": "badge-danger",
    "STARTED": "badge-warning",
    "PENDING": "badge-info",
    "RETRY": "badge-warning",
    "REVOKED": "badge-muted",
}

_POOL_META_LABELS = {
    "PID",
    "Uptime",
    "Clock",
    "Prefetch count",
    "Pool max concurrency",
    "Pool processes",
    "Max tasks per child",
}
_BROKER_META_LABELS = {
    "Broker transport",
    "Broker hostname",
    "Broker port",
}
_BACKEND_META_LABELS = {
    "Result backend",
    "Result backend transport",
}
_SYSTEM_META_LABELS = {
    "CPU user time",
    "CPU system time",
    "Max RSS",
}
_CONFIG_META_LABELS = {
    "Default queue",
    "Timezone",
    "Worker concurrency",
    "Worker pool",
    "Prefetch multiplier",
    "Task serializer",
    "Result serializer",
    "Accept content",
    "Task time limit",
    "Task soft limit",
    "Acks late",
}
_TASK_META_LABELS = {"Tasks accepted"}


class _TaskRateRow(TypedDict):
    name: str
    rate_limit: str | None
    time_limit: str | None
    soft_time_limit: str | None


class _MetaRow(TypedDict):
    label: str
    value: str


class _QueueRow(TypedDict):
    name: str
    exchange: str | None
    routing_key: str | None


class _ActiveTaskRow(TypedDict):
    name: str
    state: str
    badge_class: str


class _OverviewRow(TypedDict):
    label: str
    value: str


@dataclass(slots=True)
class _WorkerRow:
    hostname: str
    status: str
    pool_size: int
    active: int
    registered: int
    queues: list[str]
    last_seen: timedelta | None
    concurrency: int

    @property
    def badge(self) -> str:
        """Return the CSS badge class for the status."""
        return STATUS_BADGES.get(self.status, "badge-muted")


WorkerRow = _WorkerRow


def _resolve_status(status: str, active_tasks: int, last_seen: timedelta | None) -> str:
    if status.upper() == "OFFLINE":
        return "offline"
    if last_seen is not None and last_seen > _OFFLINE_THRESHOLD:
        return "offline"
    if active_tasks > 0:
        return "busy"
    return "online"


def _build_workers(now: datetime) -> list[_WorkerRow]:
    with open_db() as db:
        workers = db.get_workers()

    rows: list[_WorkerRow] = []
    for worker in workers:
        last_seen = None
        if worker.last_heartbeat is not None:
            last_seen = now - worker.last_heartbeat
        active = worker.active_tasks or 0
        status = _resolve_status(worker.status, active, last_seen)
        queues = worker.queues or []
        registered_tasks = worker.registered_tasks or []
        pool_size = worker.pool_size or 0
        rows.append(
            _WorkerRow(
                hostname=worker.hostname,
                status=status,
                pool_size=pool_size,
                active=active,
                registered=len(registered_tasks),
                queues=queues,
                last_seen=last_seen,
                concurrency=pool_size,
            ),
        )
    return rows


def fetch_workers(now: datetime) -> list[_WorkerRow]:
    """Return worker rows for views and API consumers."""
    return _build_workers(now)


def worker_list(request: HttpRequest) -> HttpResponse:
    """Render the worker overview page."""
    context = {
        "title": "Overview",
    }
    return render(request, "workers/list.html", context)


def worker_list_fragment(request: HttpRequest) -> HttpResponse:
    """Render the worker overview fragment."""
    now = timezone.now()
    workers = _build_workers(now)
    broker_groups = broker_views.list_broker_groups(include_counts=False)
    worker_lookup = {worker.hostname: worker for worker in workers}
    broker_tree: list[dict[str, object]] = []
    attached: set[str] = set()
    for broker_row in broker_groups:
        broker_workers = [worker_lookup[name] for name in broker_row.workers if name in worker_lookup]
        attached.update(worker.hostname for worker in broker_workers)
        broker_tree.append(
            {
                "broker": broker_row,
                "workers": broker_workers,
            },
        )
    unassigned_workers = [worker for worker in workers if worker.hostname not in attached]
    context = {
        "broker_tree": broker_tree,
        "unassigned_workers": unassigned_workers,
    }
    return render(request, "workers/list_content.html", context)


def _find_worker(hostname: str, workers: Sequence[_WorkerRow]) -> _WorkerRow | None:
    for worker in workers:
        if worker.hostname == hostname:
            return worker
    return None


def _parse_task_info(entry: str) -> tuple[str, dict[str, str]]:
    if " [" not in entry or not entry.endswith("]"):
        return entry, {}
    name, raw = entry.split(" [", 1)
    raw = raw[:-1]
    details: dict[str, str] = {}
    for token in raw.split():
        if "=" not in token:
            continue
        key, value = token.split("=", 1)
        details[key] = value
    return name, details


def _normalize_info(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    if normalized.lower() in {"none", "null", "n/a", ""}:
        return None
    return normalized


def _parse_task_rows(entries: Sequence[object]) -> list[_TaskRateRow]:
    rows: list[_TaskRateRow] = []
    for entry in entries:
        if not isinstance(entry, str):
            continue
        name, details = _parse_task_info(entry)
        rows.append(
            {
                "name": name,
                "rate_limit": _normalize_info(details.get("rate_limit")),
                "time_limit": _normalize_info(details.get("time_limit")),
                "soft_time_limit": _normalize_info(details.get("soft_time_limit")),
            },
        )
    rows.sort(key=lambda row: row["name"])
    return rows


def _get_nested(mapping: Mapping[str, object], *keys: str) -> object | None:
    current: object = mapping
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _format_seconds(value: object) -> str | None:
    if not isinstance(value, int | float):
        return None
    seconds = int(value)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


def _stringify(value: object) -> str:
    if isinstance(value, list | tuple | set):
        return ", ".join(str(item) for item in value)
    if isinstance(value, dict):
        return ", ".join(f"{key}={value}" for key, value in value.items())
    return str(value)


def _build_metadata_rows(
    stats: Mapping[str, object] | None,
    conf: Mapping[str, object] | None,
) -> list[_MetaRow]:
    rows: list[_MetaRow] = []

    def add(label: str, value: object | None) -> None:
        if value is None:
            return
        rows.append({"label": label, "value": _stringify(value)})

    if stats:
        add("PID", stats.get("pid"))
        add("Uptime", _format_seconds(stats.get("uptime")))
        add("Clock", stats.get("clock"))
        add("Prefetch count", stats.get("prefetch_count"))
        add("Pool max concurrency", _get_nested(stats, "pool", "max-concurrency"))
        add("Pool processes", _get_nested(stats, "pool", "processes"))
        add("Max tasks per child", _get_nested(stats, "pool", "max-tasks-per-child"))
        add("Broker transport", _get_nested(stats, "broker", "transport"))
        add("Broker hostname", _get_nested(stats, "broker", "hostname"))
        add("Broker port", _get_nested(stats, "broker", "port"))
        add("CPU user time", _get_nested(stats, "rusage", "utime"))
        add("CPU system time", _get_nested(stats, "rusage", "stime"))
        add("Max RSS", _get_nested(stats, "rusage", "maxrss"))
        totals = _get_nested(stats, "total")
        if isinstance(totals, Mapping):
            total_count = sum(int(value) for value in totals.values() if isinstance(value, int | float))
            add("Tasks accepted", total_count)

    if conf:
        add("Default queue", conf.get("task_default_queue"))
        add("Timezone", conf.get("timezone"))
        add("Worker concurrency", conf.get("worker_concurrency"))
        add("Worker pool", conf.get("worker_pool"))
        add("Prefetch multiplier", conf.get("worker_prefetch_multiplier"))
        add("Task serializer", conf.get("task_serializer"))
        add("Result backend", conf.get("result_backend"))
        add("Result backend transport", conf.get("result_backend_transport"))
        add("Result serializer", conf.get("result_serializer"))
        add("Accept content", conf.get("accept_content"))
        add("Task time limit", conf.get("task_time_limit"))
        add("Task soft limit", conf.get("task_soft_time_limit"))
        add("Acks late", conf.get("task_acks_late"))

    return rows


def _parse_queue_rows(entries: Sequence[object]) -> list[_QueueRow]:
    rows: list[_QueueRow] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            continue
        name = entry.get("name")
        exchange_value = entry.get("exchange")
        exchange = None
        if isinstance(exchange_value, Mapping):
            exchange = exchange_value.get("name")
        routing_key = entry.get("routing_key")
        rows.append(
            {
                "name": str(name) if name is not None else "—",
                "exchange": str(exchange) if exchange else None,
                "routing_key": str(routing_key) if routing_key is not None else None,
            },
        )
    rows.sort(key=lambda row: row["name"])
    return rows


def _parse_active_count(value: object) -> int | None:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, int):
        return value
    return None


def _snapshot_worker(
    hostname: str,
) -> tuple[
    list[_TaskRateRow],
    list[_QueueRow],
    list[_MetaRow],
    Mapping[str, object] | None,
    int | None,
    str | None,
    str | None,
]:
    with open_db() as db:
        snapshot = db.get_worker_event_snapshot(hostname)
    if snapshot is None:
        return [], [], [], None, None, "No worker snapshot available.", None
    info = snapshot.info
    if not isinstance(info, Mapping):
        return [], [], [], None, None, "Worker snapshot payload missing.", None
    stats = info.get("stats")
    stats_map = stats if isinstance(stats, Mapping) else info
    conf = info.get("conf")
    conf_map = conf if isinstance(conf, Mapping) else None
    queues = info.get("queues")
    registered = info.get("registered")
    active = info.get("active")
    queue_rows: list[_QueueRow] = []
    if isinstance(queues, Sequence) and not isinstance(queues, str | bytes):
        queue_rows = _parse_queue_rows(queues)
    task_rows: list[_TaskRateRow] = []
    if isinstance(registered, Sequence) and not isinstance(registered, str | bytes):
        task_rows = _parse_task_rows(registered)
    metadata_rows = _build_metadata_rows(stats_map, conf_map)
    active_count = _parse_active_count(active)
    app_name_value = info.get("app")
    inspected_app_name = str(app_name_value) if isinstance(app_name_value, str) else None
    return (
        task_rows,
        queue_rows,
        metadata_rows,
        stats_map,
        active_count,
        None,
        inspected_app_name,
    )


def _fetch_active_rows(hostname: str) -> list[_ActiveTaskRow]:
    with open_db() as db:
        active_task_rows = db.get_tasks(TaskFilter(worker=hostname, state="STARTED"))
    return [
        {
            "name": task.name or task.task_id,
            "state": task.state,
            "badge_class": TASK_BADGES.get(task.state, "badge-muted"),
        }
        for task in active_task_rows
    ]


def worker_detail(request: HttpRequest, hostname: str) -> HttpResponse:
    """Render the worker detail shell page."""
    worker = _get_worker_or_404(hostname)
    context = {
        "title": "Worker detail",
        "worker": worker,
    }
    return render(request, "workers/detail.html", context)


def worker_detail_fragment(request: HttpRequest, hostname: str) -> HttpResponse:
    """Render the worker detail fragment."""
    worker = _get_worker_or_404(hostname)
    active_rows = _fetch_active_rows(hostname)
    last_seen = worker.last_seen.total_seconds() if worker.last_seen is not None else None
    task_rows: list[_TaskRateRow]
    queue_rows: list[_QueueRow]
    metadata_rows: list[_MetaRow]
    stats_map: Mapping[str, object] | None
    active_count: int | None
    inspect_error: str | None
    inspected_app_name: str | None

    if worker.status == "offline":
        task_rows, queue_rows, metadata_rows, stats_map, active_count, inspected_app_name = [], [], [], None, None, None
        inspect_error = "Worker is offline; snapshot unavailable."
    else:
        (
            task_rows,
            queue_rows,
            metadata_rows,
            stats_map,
            active_count,
            inspect_error,
            inspected_app_name,
        ) = _snapshot_worker(hostname)

    pool_size = _resolve_pool_size(worker, stats_map)
    concurrency = worker.concurrency or pool_size
    active_tasks = worker.active or active_count
    registered = _resolve_registered_count(worker, stats_map)
    overview = _build_overview(pool_size, active_tasks, registered, concurrency)

    (
        pool_rows,
        broker_rows,
        backend_rows,
        system_rows,
        config_rows,
        task_meta_rows,
        other_rows,
    ) = _split_metadata_rows(metadata_rows)

    broker_queue_rows, broker_key, broker_label, broker_type_label = _resolve_broker_detail(inspected_app_name)

    context = {
        "worker": worker,
        "overview": overview,
        "queues": worker.queues,
        "last_seen": last_seen,
        "active_tasks": active_rows,
        "task_rows": task_rows,
        "queue_rows": queue_rows,
        "pool_rows": pool_rows,
        "broker_rows": broker_rows,
        "broker_queue_rows": broker_queue_rows,
        "broker_key": broker_key,
        "broker_label": broker_label,
        "broker_type_label": broker_type_label,
        "backend_rows": backend_rows,
        "system_rows": system_rows,
        "config_rows": config_rows,
        "task_meta_rows": task_meta_rows,
        "other_rows": other_rows,
        "inspect_error": inspect_error,
    }
    return render(request, "workers/detail_content.html", context)


def _resolve_pool_size(worker: _WorkerRow, stats_map: Mapping[str, object] | None) -> int | None:
    if worker.pool_size is not None:
        return worker.pool_size
    candidate = _get_nested(stats_map or {}, "pool", "max-concurrency")
    if isinstance(candidate, int):
        return candidate
    candidate = _get_nested(stats_map or {}, "pool", "max_concurrency")
    if isinstance(candidate, int):
        return candidate
    return None


def _resolve_registered_count(worker: _WorkerRow, stats_map: Mapping[str, object] | None) -> int | None:
    if worker.registered == 0 and isinstance(stats_map, Mapping):
        totals = _get_nested(stats_map, "total")
        if isinstance(totals, Mapping):
            return sum(1 for value in totals.values() if isinstance(value, int))
    return worker.registered


def _format_overview_value(value: object | None) -> str:
    return "—" if value is None else str(value)


def _build_overview(
    pool_size: int | None,
    active_tasks: int | None,
    registered: int | None,
    concurrency: int | None,
) -> list[_OverviewRow]:
    return [
        {"label": "Pool size", "value": _format_overview_value(pool_size)},
        {"label": "Active tasks", "value": _format_overview_value(active_tasks)},
        {"label": "Registered", "value": _format_overview_value(registered)},
        {"label": "Concurrency", "value": _format_overview_value(concurrency)},
    ]


def _split_metadata_rows(
    metadata_rows: Sequence[_MetaRow],
) -> tuple[
    list[_MetaRow],
    list[_MetaRow],
    list[_MetaRow],
    list[_MetaRow],
    list[_MetaRow],
    list[_MetaRow],
    list[_MetaRow],
]:
    pool_rows: list[_MetaRow] = []
    broker_rows: list[_MetaRow] = []
    backend_rows: list[_MetaRow] = []
    system_rows: list[_MetaRow] = []
    config_rows: list[_MetaRow] = []
    task_meta_rows: list[_MetaRow] = []
    other_rows: list[_MetaRow] = []
    for row in metadata_rows:
        label = row["label"]
        if label in _POOL_META_LABELS:
            pool_rows.append(row)
        elif label in _BROKER_META_LABELS:
            broker_rows.append(row)
        elif label in _BACKEND_META_LABELS:
            backend_rows.append(row)
        elif label in _SYSTEM_META_LABELS:
            system_rows.append(row)
        elif label in _CONFIG_META_LABELS:
            config_rows.append(row)
        elif label in _TASK_META_LABELS:
            task_meta_rows.append(row)
        else:
            other_rows.append(row)
    return (
        pool_rows,
        broker_rows,
        backend_rows,
        system_rows,
        config_rows,
        task_meta_rows,
        other_rows,
    )


def _resolve_broker_detail(
    inspected_app_name: str | None,
) -> tuple[list[broker_views.QueueRow], str | None, str | None, str | None]:
    if not inspected_app_name:
        return [], None, None, None
    registry = get_registry()
    try:
        app = registry.get_app(inspected_app_name)
    except KeyError:
        return [], None, None, None
    broker_url = str(app.conf.broker_url or "")
    broker_queue_rows, _latest = broker_views.queue_rows_for_broker_snapshot(broker_url)
    broker_key = broker_views.encode_broker_key(broker_url)
    broker_label = broker_url or "default"
    broker_type_label = broker_views.broker_type_label(broker_url)
    return broker_queue_rows, broker_key, broker_label, broker_type_label


def _get_worker_or_404(hostname: str) -> _WorkerRow:
    now = timezone.now()
    workers = _build_workers(now)
    worker = _find_worker(hostname, workers)
    if worker is None:
        raise Http404(_WORKER_NOT_FOUND)
    return worker


def _default_worker_name() -> str | None:
    registry = get_registry()
    apps = registry.get_apps()
    if not apps:
        return None
    return app_name(apps[0])


@require_post
def workers_restart(_request: HttpRequest) -> HttpResponse:
    """Restart all workers via remote control."""
    registry = get_registry()
    worker_name = _default_worker_name()
    if worker_name is None:
        return HttpResponseBadRequest("No Celery workers configured")
    worker_control.restart(registry, worker_name)
    return redirect("workers")


@require_post
def worker_grow(_request: HttpRequest, hostname: str) -> HttpResponse:
    """Increase the pool size for a worker."""
    registry = get_registry()
    worker_name = _default_worker_name()
    if worker_name is None:
        return HttpResponseBadRequest("No Celery workers configured")
    worker_control.pool_grow(registry, worker_name, destination=[hostname])
    return redirect("worker-detail", hostname=hostname)


@require_post
def worker_shrink(_request: HttpRequest, hostname: str) -> HttpResponse:
    """Decrease the pool size for a worker."""
    registry = get_registry()
    worker_name = _default_worker_name()
    if worker_name is None:
        return HttpResponseBadRequest("No Celery workers configured")
    worker_control.pool_shrink(registry, worker_name, destination=[hostname])
    return redirect("worker-detail", hostname=hostname)


@require_post
def worker_autoscale(_request: HttpRequest, hostname: str) -> HttpResponse:
    """Enable autoscale on a worker using current pool size as max."""
    registry = get_registry()
    worker_name = _default_worker_name()
    if worker_name is None:
        return HttpResponseBadRequest("No Celery workers configured")
    with open_db() as db:
        worker = db.get_worker(hostname)
    max_concurrency = worker.pool_size if worker and worker.pool_size else 1
    worker_control.autoscale(
        registry,
        worker_name,
        max_concurrency=max_concurrency,
        min_concurrency=0,
        destination=[hostname],
    )
    return redirect("worker-detail", hostname=hostname)


def worker_add(request: HttpRequest) -> HttpResponse:
    """Start a new local worker process for configured apps."""
    options = list_worker_options()
    allowed_modules = {option.module for option in options}
    if request.method == "POST":
        module = request.POST.get("module", "").strip()
        queue = request.POST.get("queue", "").strip()
        name = request.POST.get("name", "").strip()
        concurrency_raw = request.POST.get("concurrency", "").strip()
        try:
            concurrency = int(concurrency_raw) if concurrency_raw else None
        except ValueError:
            concurrency = None

        if not module:
            return HttpResponseBadRequest("Worker module is required")
        if module not in allowed_modules:
            return HttpResponseBadRequest("Unknown worker module")

        command = ["uv", "run", "celery", "-A", module, "worker"]
        if queue:
            command.extend(["-Q", queue])
        if name:
            command.extend(["-n", name])
        if concurrency is not None:
            command.extend(["-c", str(concurrency)])
        command.extend(["-l", "INFO"])
        subprocess.Popen(command, start_new_session=True)  # noqa: S603
        return redirect("workers")

    return render(
        request,
        "workers/add.html",
        {
            "title": "Add worker",
            "options": options,
        },
    )
