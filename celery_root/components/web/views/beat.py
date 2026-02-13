# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Beat schedule page views."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from django.http import Http404, HttpResponseBadRequest
from django.shortcuts import redirect, render
from django.utils import timezone

from celery_root.components.beat import BeatController
from celery_root.components.web.services import app_name, get_registry, list_task_names, open_db
from celery_root.components.web.views.tasks import build_task_schemas
from celery_root.config import get_settings
from celery_root.core.db.models import Schedule

from .decorators import require_post

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import datetime

    from celery import Celery
    from django.http import HttpRequest, HttpResponse

_SCHEDULE_NOT_FOUND = "Schedule not found"
_BEAT_DISABLED = "Beat is disabled."


@dataclass(slots=True)
class _ScheduleRow:
    id: str
    name: str
    task: str
    schedule: str
    args: str | None
    kwargs: str | None
    enabled: bool
    last_run: datetime | None
    total_runs: int | None
    app: str | None


def _ensure_beat_enabled() -> None:
    if get_settings().beat is None:
        raise Http404(_BEAT_DISABLED)


def _list_schedules() -> list[_ScheduleRow]:
    registry = get_registry()
    apps = registry.get_apps()
    if not apps:
        return []
    rows: list[_ScheduleRow] = []
    with open_db() as db:
        for app in apps:
            controller = BeatController(app, db)
            try:
                schedules = controller.list_schedules()
            except RuntimeError:
                schedules = []
            for schedule in schedules:
                if schedule.app is None:
                    schedule.app = app_name(app)
                rows.append(
                    _ScheduleRow(
                        id=schedule.schedule_id,
                        name=schedule.name,
                        task=schedule.task,
                        schedule=schedule.schedule,
                        args=schedule.args,
                        kwargs=schedule.kwargs,
                        enabled=schedule.enabled,
                        last_run=schedule.last_run_at,
                        total_runs=schedule.total_run_count,
                        app=schedule.app,
                    ),
                )
    return rows


def _find_app(apps: Sequence[Celery], label: str | None) -> Celery | None:
    if label is None:
        return None
    for app in apps:
        if app_name(app) == label:
            return app
    return None


def list_schedules() -> list[_ScheduleRow]:
    """Expose beat schedules for API consumers."""
    _ensure_beat_enabled()
    return _list_schedules()


def beat(request: HttpRequest) -> HttpResponse:
    """Render the beat schedule page."""
    _ensure_beat_enabled()
    schedules = list_schedules()
    context = {
        "title": "Beat schedule",
        "schedules": schedules,
        "now": timezone.now(),
    }
    return render(request, "beat.html", context)


def _parse_form(request: HttpRequest, schedule_id: str | None) -> Schedule:
    name = request.POST.get("name", "").strip()
    worker_label = request.POST.get("worker", "").strip()
    task_name = request.POST.get("task", "").strip()
    schedule_expr = request.POST.get("schedule", "").strip()
    if not name or not task_name or not schedule_expr or not worker_label:
        message = "name, worker, task, and schedule are required"
        raise ValueError(message)
    schedule_id_value = schedule_id or request.POST.get("schedule_id", "").strip() or name
    args = request.POST.get("args", "").strip() or None
    kwargs = request.POST.get("kwargs", "").strip() or None
    enabled = bool(request.POST.get("enabled"))
    return Schedule(
        schedule_id=schedule_id_value,
        name=name,
        app=worker_label,
        task=task_name,
        schedule=schedule_expr,
        args=args,
        kwargs=kwargs,
        enabled=enabled,
        last_run_at=None,
        total_run_count=None,
    )


def _render_form(request: HttpRequest, schedule: Schedule | _ScheduleRow | None, *, title: str) -> HttpResponse:
    registry = get_registry()
    apps = registry.get_apps()
    worker_labels = [app_name(app) for app in apps]
    selected_worker = (
        request.GET.get("app")
        or (schedule.app if schedule is not None else None)
        or (worker_labels[0] if worker_labels else None)
    )
    task_schemas: dict[str, object] = {}
    for app in apps:
        label = app_name(app)
        task_names = list_task_names((app,))
        task_schemas[label] = build_task_schemas((app,), task_names)
    context = {
        "title": title,
        "schedule": schedule,
        "worker_labels": worker_labels,
        "selected_worker": selected_worker,
        "task_schemas": task_schemas,
    }
    return render(request, "beat/form.html", context)


def beat_add(request: HttpRequest) -> HttpResponse:
    """Create a new beat schedule."""
    _ensure_beat_enabled()
    if request.method == "POST":
        try:
            schedule = _parse_form(request, None)
        except ValueError as exc:
            return HttpResponseBadRequest(str(exc))
        with open_db() as db:
            registry = get_registry()
            apps = registry.get_apps()
            if not apps:
                return HttpResponseBadRequest("No Celery workers configured")
            target = _find_app(apps, schedule.app)
            if target is None:
                return HttpResponseBadRequest("Unknown worker selection")
            controller = BeatController(target, db)
            controller.save_schedule(schedule)
        return redirect("beat")
    return _render_form(request, None, title="Add schedule")


def beat_edit(request: HttpRequest, schedule_id: str) -> HttpResponse:
    """Edit an existing beat schedule."""
    _ensure_beat_enabled()
    schedules = _list_schedules()
    requested_app = (request.GET.get("app") or request.POST.get("worker") or "").strip() or None
    schedule = next(
        (item for item in schedules if item.id == schedule_id and (requested_app is None or item.app == requested_app)),
        None,
    )
    if schedule is None:
        raise Http404(_SCHEDULE_NOT_FOUND)

    if request.method == "POST":
        try:
            parsed = _parse_form(request, schedule_id)
        except ValueError as exc:
            return HttpResponseBadRequest(str(exc))
        with open_db() as db:
            registry = get_registry()
            apps = registry.get_apps()
            if not apps:
                return HttpResponseBadRequest("No Celery workers configured")
            target = _find_app(apps, parsed.app)
            if target is None:
                return HttpResponseBadRequest("Unknown worker selection")
            if schedule.app and schedule.app != parsed.app:
                previous = _find_app(apps, schedule.app)
                if previous is not None:
                    BeatController(previous, db).delete_schedule(schedule_id)
            BeatController(target, db).save_schedule(parsed)
        return redirect("beat")

    return _render_form(request, schedule, title="Edit schedule")


@require_post
def beat_delete(_request: HttpRequest, schedule_id: str) -> HttpResponse:
    """Delete a beat schedule."""
    _ensure_beat_enabled()
    with open_db() as db:
        registry = get_registry()
        apps = registry.get_apps()
        if not apps:
            return HttpResponseBadRequest("No Celery workers configured")
        selected_app = (_request.POST.get("app") or _request.GET.get("app") or "").strip() or None
        target = apps[0] if selected_app is None else _find_app(apps, selected_app)
        if target is None:
            return HttpResponseBadRequest("Unknown worker selection")
        BeatController(target, db).delete_schedule(schedule_id)
    return redirect("beat")


@require_post
def beat_sync(_request: HttpRequest) -> HttpResponse:
    """Sync beat schedules into the database."""
    _ensure_beat_enabled()
    with open_db() as db:
        registry = get_registry()
        apps = registry.get_apps()
        if not apps:
            return HttpResponseBadRequest("No Celery workers configured")
        for app in apps:
            BeatController(app, db).sync_to_db()
    return redirect("beat")
