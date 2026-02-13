# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Beat schedule synchronization for Celery Root."""

from __future__ import annotations

import importlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, cast

from celery.beat import PersistentScheduler, ScheduleEntry
from celery.schedules import crontab
from celery.schedules import schedule as interval_schedule

from celery_root.core.db.models import Schedule

if TYPE_CHECKING:
    from collections.abc import Iterable

    from celery import Celery

    from celery_root.core.db.adapters.base import BaseDBController

    class _CrontabSpec(Protocol):
        minute: str
        hour: str
        day_of_month: str
        month_of_year: str
        day_of_week: str

    class _IntervalSpecModel(Protocol):
        every: int
        period: str

    class _PeriodicTaskModel(Protocol):
        objects: _PeriodicTaskQuerySet
        id: int
        name: str
        task: str
        args: str | None
        kwargs: str | None
        enabled: bool
        last_run_at: datetime | None
        total_run_count: int | None
        crontab_id: int | None
        interval_id: int | None
        crontab: _CrontabSpec | None
        interval: _IntervalSpecModel | None

        def __init__(self, *, name: str, task: str) -> None: ...
        def save(self) -> None: ...

    class _PeriodicTaskQuerySet(Protocol):
        def all(self) -> Iterable[_PeriodicTaskModel]: ...
        def filter(self, **kwargs: object) -> _PeriodicTaskQuerySet: ...
        def delete(self) -> tuple[int, dict[str, int]]: ...
        def first(self) -> _PeriodicTaskModel | None: ...

    class _ScheduleManager(Protocol):
        def get_or_create(self, **kwargs: object) -> tuple[object, bool]: ...

    class _CrontabScheduleModel(Protocol):
        objects: _ScheduleManager

    class _IntervalScheduleModel(Protocol):
        objects: _ScheduleManager

    class _PeriodicTasksModel(Protocol):
        @classmethod
        def changed(cls) -> None: ...

    class DjangoBeatModels(Protocol):
        PeriodicTask: type[_PeriodicTaskModel]
        CrontabSchedule: type[_CrontabScheduleModel]
        IntervalSchedule: type[_IntervalScheduleModel]
        PeriodicTasks: type[_PeriodicTasksModel]


__all__ = ["BeatController"]

_CRON_FIELD_COUNT = 5
_INTERVAL_PREFIX = "interval:"
_INTERVAL_RE = re.compile(r"every\s+(?P<seconds>[\d.]+)\s+seconds")
_UNIT_SECONDS = {
    "days": 86400,
    "hours": 3600,
    "minutes": 60,
    "seconds": 1,
    "microseconds": 0.000001,
}
_DJANGO_BEAT_UNAVAILABLE = "django-celery-beat is not available"


class _ScheduleEntryLike(Protocol):
    schedule: crontab | interval_schedule
    task: str
    args: tuple[object, ...]
    kwargs: dict[str, object]
    options: dict[str, object] | None
    last_run_at: datetime | None
    total_run_count: int | None


class _SchedulerLike(Protocol):
    schedule: dict[str, _ScheduleEntryLike]

    def setup_schedule(self) -> None: ...
    def sync(self) -> None: ...
    def close(self) -> None: ...


@dataclass(slots=True)
class BeatBackend:
    """Descriptor for the active beat backend."""

    name: str
    scheduler: str


class BeatController:
    """Integrates with Celery beat backends (file-based or django-celery-beat)."""

    def __init__(self, app: Celery, db: BaseDBController | None = None) -> None:
        """Initialize the controller for a Celery app."""
        self._app = app
        self._app_label = _resolve_app_name(app)
        self._db = db

    def detect_backend(self) -> BeatBackend:
        """Detect the active beat backend for the app."""
        scheduler = str(self._app.conf.get("beat_scheduler") or "")
        if "django_celery_beat" in scheduler:
            return BeatBackend(name="django_celery_beat", scheduler=scheduler)
        if scheduler:
            return BeatBackend(name="file", scheduler=scheduler)
        return BeatBackend(name="file", scheduler="celery.beat.PersistentScheduler")

    def list_schedules(self) -> list[Schedule]:
        """List schedules from the configured backend."""
        backend = self.detect_backend()
        if backend.name == "django_celery_beat":
            schedules = self._list_django_schedules()
        else:
            schedules = self._list_file_schedules()
        return schedules

    def sync_to_db(self) -> list[Schedule]:
        """Sync backend schedules into the DB."""
        schedules = self.list_schedules()
        if self._db is not None:
            for schedule in schedules:
                self._db.store_schedule(schedule)
        return schedules

    def save_schedule(self, schedule: Schedule) -> None:
        """Save or update a schedule in the backend and DB."""
        backend = self.detect_backend()
        if backend.name == "django_celery_beat":
            self._save_django_schedule(schedule)
        else:
            self._save_file_schedule(schedule)
        if self._db is not None:
            self._db.store_schedule(schedule)

    def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule from the backend and DB."""
        backend = self.detect_backend()
        if backend.name == "django_celery_beat":
            self._delete_django_schedule(schedule_id)
        else:
            self._delete_file_schedule(schedule_id)
        if self._db is not None:
            self._db.delete_schedule(schedule_id)

    def reload(self) -> None:
        """Reload the scheduler state when the backend supports it."""
        backend = self.detect_backend()
        if backend.name == "django_celery_beat":
            self._django_changed()
        else:
            scheduler = self._file_scheduler()
            scheduler.sync()
            scheduler.close()

    def _file_scheduler(self) -> _SchedulerLike:
        schedule_filename = self._app.conf.get("beat_schedule_filename") or "celerybeat-schedule"
        scheduler = cast("Any", PersistentScheduler)(
            app=self._app,
            schedule_filename=str(schedule_filename),
        )
        return cast("_SchedulerLike", scheduler)

    def _list_file_schedules(self) -> list[Schedule]:
        scheduler = self._file_scheduler()
        scheduler.setup_schedule()
        schedules: list[Schedule] = []
        for name, entry in scheduler.schedule.items():
            schedules.append(self._schedule_from_entry(name, entry))
        scheduler.sync()
        scheduler.close()
        return schedules

    def _save_file_schedule(self, schedule: Schedule) -> None:
        entry_name = schedule.schedule_id or schedule.name
        schedule_obj = _parse_schedule(schedule.schedule)
        args = _parse_args(schedule.args)
        kwargs = _parse_kwargs(schedule.kwargs)
        self._update_app_schedule(
            entry_name,
            schedule.task,
            schedule_obj,
            args,
            kwargs,
            enabled=schedule.enabled,
        )
        scheduler = self._file_scheduler()
        scheduler.setup_schedule()
        entry = cast(
            "_ScheduleEntryLike",
            cast("Any", ScheduleEntry)(
                name=entry_name,
                task=schedule.task,
                schedule=schedule_obj,
                args=args,
                kwargs=kwargs,
                options={"enabled": schedule.enabled},
                last_run_at=schedule.last_run_at or self._app.now(),
                total_run_count=schedule.total_run_count or 0,
                app=self._app,
            ),
        )
        scheduler.schedule[entry_name] = entry
        scheduler.sync()
        scheduler.close()

    def _delete_file_schedule(self, schedule_id: str) -> None:
        scheduler = self._file_scheduler()
        scheduler.setup_schedule()
        if schedule_id in scheduler.schedule:
            del scheduler.schedule[schedule_id]
        self._delete_from_app_schedule(schedule_id)
        scheduler.sync()
        scheduler.close()

    def _schedule_from_entry(self, name: str, entry: _ScheduleEntryLike) -> Schedule:
        schedule_str = _format_schedule(entry.schedule)
        return Schedule(
            schedule_id=name,
            name=name,
            task=str(entry.task),
            schedule=schedule_str,
            args=_dump_json(entry.args),
            kwargs=_dump_json(entry.kwargs),
            enabled=bool(entry.options.get("enabled", True)) if entry.options else True,
            last_run_at=_coerce_datetime(entry.last_run_at),
            total_run_count=int(entry.total_run_count or 0),
            app=self._app_label,
        )

    def _list_django_schedules(self) -> list[Schedule]:
        periodic_task_model = self._django_models().PeriodicTask
        schedules: list[Schedule] = []
        for task in periodic_task_model.objects.all():
            schedule_str = _format_django_schedule(task)
            if schedule_str is None:
                continue
            schedules.append(
                Schedule(
                    schedule_id=str(task.id),
                    name=task.name,
                    task=task.task,
                    schedule=schedule_str,
                    args=task.args,
                    kwargs=task.kwargs,
                    enabled=bool(task.enabled),
                    last_run_at=_coerce_datetime(task.last_run_at),
                    total_run_count=int(task.total_run_count or 0),
                    app=self._app_label,
                ),
            )
        return schedules

    def _save_django_schedule(self, schedule: Schedule) -> None:
        models = self._django_models()
        periodic_task_model = models.PeriodicTask
        periodic = _get_periodic_task(periodic_task_model, schedule.schedule_id, schedule.name)
        periodic.task = schedule.task
        periodic.enabled = schedule.enabled
        periodic.args = schedule.args or "[]"
        periodic.kwargs = schedule.kwargs or "{}"

        cron_fields = _parse_cron_fields(schedule.schedule)
        if cron_fields is not None:
            crontab_entry, _ = models.CrontabSchedule.objects.get_or_create(
                minute=cron_fields.minute,
                hour=cron_fields.hour,
                day_of_week=cron_fields.day_of_week,
                day_of_month=cron_fields.day_of_month,
                month_of_year=cron_fields.month_of_year,
                timezone=str(self._app.conf.get("timezone") or "UTC"),
            )
            periodic.crontab = cast("_CrontabSpec", crontab_entry)
            periodic.interval = None
        else:
            seconds = _parse_interval_seconds(schedule.schedule)
            interval_entry, _ = models.IntervalSchedule.objects.get_or_create(
                every=int(seconds.every),
                period=seconds.period,
            )
            periodic.interval = cast("_IntervalSpecModel", interval_entry)
            periodic.crontab = None

        periodic.save()
        self._django_changed()

    def _delete_django_schedule(self, schedule_id: str) -> None:
        periodic_task_model = self._django_models().PeriodicTask
        deleted = periodic_task_model.objects.filter(id=_safe_int(schedule_id)).delete()
        if deleted[0] == 0:
            periodic_task_model.objects.filter(name=schedule_id).delete()
        self._django_changed()

    def _django_models(self) -> DjangoBeatModels:
        try:
            settings_module = importlib.import_module("django.conf")
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise _django_unavailable_error() from exc
        settings = settings_module.settings
        if not settings.configured:
            message = "Django settings are not configured"
            raise RuntimeError(message)
        try:
            django = importlib.import_module("django")
            django.setup()
            models = importlib.import_module("django_celery_beat.models")
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise _django_unavailable_error() from exc
        return cast("DjangoBeatModels", models)

    def _django_changed(self) -> None:
        try:
            models = self._django_models()
            models.PeriodicTasks.changed()
        except RuntimeError:
            return

    def _update_app_schedule(  # noqa: PLR0913
        self,
        name: str,
        task: str,
        schedule_obj: crontab | interval_schedule,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        *,
        enabled: bool,
    ) -> None:
        beat_schedule = dict(self._app.conf.get("beat_schedule") or {})
        beat_schedule[name] = {
            "task": task,
            "schedule": schedule_obj,
            "args": args,
            "kwargs": kwargs,
            "options": {"enabled": enabled},
        }
        self._app.conf.beat_schedule = beat_schedule

    def _delete_from_app_schedule(self, name: str) -> None:
        beat_schedule = dict(self._app.conf.get("beat_schedule") or {})
        if name in beat_schedule:
            beat_schedule.pop(name, None)
            self._app.conf.beat_schedule = beat_schedule


def _django_unavailable_error() -> RuntimeError:
    """Build a standardized error for missing django-celery-beat."""
    return RuntimeError(_DJANGO_BEAT_UNAVAILABLE)


def _resolve_app_name(app: Celery) -> str:
    raw_main = getattr(app, "main", None)
    name = str(raw_main) if raw_main else ""
    if not name:
        conf_main = app.conf.get("main")
        name = str(conf_main) if conf_main else ""
    if not name:
        name = f"celery_app_{id(app)}"
    return name


def _parse_schedule(value: str) -> crontab | interval_schedule:
    cron_fields = _parse_cron_fields(value)
    if cron_fields is not None:
        return crontab(
            minute=cron_fields.minute,
            hour=cron_fields.hour,
            day_of_week=cron_fields.day_of_week,
            day_of_month=cron_fields.day_of_month,
            month_of_year=cron_fields.month_of_year,
        )
    seconds = _parse_interval_seconds(value)
    interval_seconds = seconds.every * seconds.unit_seconds
    return interval_schedule(run_every=timedelta(seconds=interval_seconds))


def _format_schedule(schedule_obj: object) -> str:
    if isinstance(schedule_obj, crontab):
        return str(schedule_obj)
    run_every = getattr(schedule_obj, "run_every", None)
    if isinstance(run_every, timedelta):
        return f"{_INTERVAL_PREFIX}{run_every.total_seconds()}"
    return str(schedule_obj)


def _parse_cron_fields(value: str) -> _CronFields | None:
    parts = value.strip().split()
    if len(parts) >= _CRON_FIELD_COUNT:
        return _CronFields(
            raw=value.strip(),
            minute=parts[0],
            hour=parts[1],
            day_of_month=parts[2],
            month_of_year=parts[3],
            day_of_week=parts[4],
        )
    return None


@dataclass(slots=True)
class _IntervalSpec:
    every: float
    period: str
    unit_seconds: float


def _parse_interval_seconds(value: str) -> _IntervalSpec:
    raw = value.strip()
    if raw.startswith(_INTERVAL_PREFIX):
        raw = raw.removeprefix(_INTERVAL_PREFIX)
    match = _INTERVAL_RE.search(raw)
    if match:
        seconds = float(match.group("seconds"))
        return _IntervalSpec(every=seconds, period="seconds", unit_seconds=1.0)
    try:
        seconds = float(raw)
        return _IntervalSpec(every=seconds, period="seconds", unit_seconds=1.0)
    except ValueError:
        return _IntervalSpec(every=60.0, period="seconds", unit_seconds=1.0)


def _parse_args(value: str | None) -> tuple[object, ...]:
    if not value:
        return ()
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return (value,)
    if isinstance(parsed, list):
        return tuple(parsed)
    return (parsed,)


def _parse_kwargs(value: str | None) -> dict[str, object]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


def _dump_json(value: object) -> str | None:
    if value is None:
        return None
    try:
        return json.dumps(value)
    except TypeError:
        return str(value)


def _coerce_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _safe_int(value: str) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


@dataclass(slots=True)
class _CronFields:
    raw: str
    minute: str
    hour: str
    day_of_month: str
    month_of_year: str
    day_of_week: str


def _format_django_schedule(task: _PeriodicTaskModel) -> str | None:
    if getattr(task, "crontab_id", None):
        cron = task.crontab
        if cron is None:
            return None
        return f"{cron.minute} {cron.hour} {cron.day_of_month} {cron.month_of_year} {cron.day_of_week}"
    if getattr(task, "interval_id", None):
        interval = task.interval
        if interval is None:
            return None
        unit_seconds = _UNIT_SECONDS.get(interval.period, 1)
        return f"{_INTERVAL_PREFIX}{interval.every * unit_seconds}"
    return None


def _get_periodic_task(
    periodic_task_model: type[_PeriodicTaskModel],
    schedule_id: str,
    name: str,
) -> _PeriodicTaskModel:
    lookup_id = _safe_int(schedule_id)
    periodic = None
    if lookup_id is not None:
        periodic = periodic_task_model.objects.filter(id=lookup_id).first()
    if periodic is None:
        periodic = periodic_task_model.objects.filter(name=name).first()
    if periodic is None:
        periodic = periodic_task_model(name=name, task=name)
    return periodic
