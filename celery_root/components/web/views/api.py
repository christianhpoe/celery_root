# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""JSON API views for Celery Root."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from django.http import Http404, JsonResponse
from django.utils import timezone

from celery_root.components.web.services import open_db
from celery_root.core.db.models import TimeRange

from . import beat as beat_views
from . import dashboard as dashboard_views
from . import tasks as task_views

if TYPE_CHECKING:
    from django.http import HttpRequest

_TASK_NOT_FOUND = "Task not found"
_HEATMAP_DAYS = 7


def _format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.isoformat()


def _serialize_task(task: task_views.TaskEntry | task_views.TaskView) -> dict[str, object]:
    timestamp = task["timestamp"] if isinstance(task["timestamp"], datetime) else None
    return {
        "task_id": task["task_id"],
        "name": task["name"],
        "state": task["state"],
        "worker": task["worker"],
        "runtime": task["runtime"],
        "received": _format_timestamp(task.get("received")),
        "started": _format_timestamp(task.get("started")),
        "finished": _format_timestamp(task.get("finished")),
        "done": task.get("done"),
        "timestamp": _format_timestamp(timestamp),
        "args": task["args"],
        "kwargs": task["kwargs"],
        "retries": task["retries"],
        "result": task["result"],
        "traceback": task["traceback"],
        "parent_id": task["parent_id"],
        "root_id": task["root_id"],
        "group_id": task["group_id"],
        "chord_id": task["chord_id"],
        "eta": task["eta"],
    }


def _serialize_worker(worker: dashboard_views._WorkerSummary | dict[str, object]) -> dict[str, object]:
    return {
        "hostname": worker["hostname"],
        "status": worker["status"],
        "badge": worker["badge"],
        "state_cells": worker.get("state_cells", []),
        "last_seen_seconds": worker.get("last_seen_seconds"),
        "pool_size": worker.get("pool_size"),
        "active": worker.get("active"),
        "processed": worker.get("processed"),
        "registered": worker.get("registered"),
        "queues": worker.get("queues"),
        "concurrency": worker.get("concurrency"),
    }


def tasks(_request: HttpRequest) -> JsonResponse:
    """Return recent tasks."""
    built = task_views.build_tasks()
    return JsonResponse({"tasks": [_serialize_task(item) for item in built]})


def task_detail(_request: HttpRequest, task_id: str) -> JsonResponse:
    """Return details for a single task."""
    task = task_views.fetch_task(task_id)
    if task is None:
        raise Http404(_TASK_NOT_FOUND)
    return JsonResponse(_serialize_task(task))


def task_relations(_request: HttpRequest, task_id: str) -> JsonResponse:
    """Return relations for a single task."""
    with open_db() as db:
        task = db.get_task(task_id)
        if task is None:
            raise Http404(_TASK_NOT_FOUND)
        root_id = task.root_id or task.task_id
        relations = db.get_task_relations(root_id)
    payload = [{"parent": rel.parent_id, "child": rel.child_id, "relation": rel.relation} for rel in relations]
    return JsonResponse({"relations": payload})


def worker_list(_request: HttpRequest) -> JsonResponse:
    """Return worker data."""
    now = timezone.now()
    workers = dashboard_views.worker_rows(now)
    return JsonResponse({"workers": [_serialize_worker(worker) for worker in workers]})


def stats_throughput(_request: HttpRequest) -> JsonResponse:
    """Return throughput data."""
    now = timezone.now()
    series = dashboard_views.throughput_series(now)
    return JsonResponse({"series": series})


def stats_state(_request: HttpRequest) -> JsonResponse:
    """Return state distribution data."""
    now = timezone.now()
    state_cards = dashboard_views.state_cards(now)
    return JsonResponse({"states": dashboard_views.state_series(state_cards)})


def stats_heatmap(_request: HttpRequest) -> JsonResponse:
    """Return heatmap data."""
    now = timezone.now()
    time_range = TimeRange(start=now - timedelta(days=7), end=now)
    with open_db() as db:
        heatmap = db.get_heatmap(time_range)
    if len(heatmap) == _HEATMAP_DAYS:
        heatmap = heatmap[-1:] + heatmap[:-1]
    return JsonResponse({"heatmap": heatmap})


def events_latest(_request: HttpRequest) -> JsonResponse:
    """Return recent event feed data."""
    now = timezone.now()
    events = [
        {
            "task": payload["task"],
            "state": payload["state"],
            "worker": payload["worker"],
            "timestamp": payload["timestamp"].isoformat(),
        }
        for payload in dashboard_views.activity_feed(now)
    ]
    return JsonResponse({"events": events})


def beat_schedules(_request: HttpRequest) -> JsonResponse:
    """Return beat schedule data."""
    schedules = beat_views.list_schedules()
    payload = [
        {
            "id": schedule.id,
            "app": schedule.app,
            "task": schedule.task,
            "name": schedule.name,
            "schedule": schedule.schedule,
            "enabled": schedule.enabled,
        }
        for schedule in schedules
    ]
    return JsonResponse({"schedules": payload})
