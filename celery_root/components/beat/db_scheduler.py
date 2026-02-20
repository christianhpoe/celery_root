# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""DB-backed Celery beat scheduler using the Celery Root RPC store."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any, Protocol, cast

from celery.beat import ScheduleEntry, Scheduler

from celery_root.config import get_settings
from celery_root.core.db.rpc_client import DbRpcClient

from .controller import _parse_args, _parse_kwargs, _parse_schedule, _resolve_app_name

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime

    from celery import Celery

    from celery_root.core.db.models import Schedule


class _EntryLike(Protocol):
    last_run_at: datetime | None
    total_run_count: int | None


_DEFAULT_REFRESH_SECONDS = 5.0
_REFRESH_CONF_KEY = "beat_db_refresh_seconds"


class DatabaseScheduler(Scheduler):
    """Celery beat scheduler that reads/writes schedules from the Root DB."""

    app: Celery

    def __init__(self, *args: object, **kwargs: object) -> None:
        """Initialize the scheduler with optional DB settings."""
        self._logger = logging.getLogger(__name__)
        self._db_client = cast("DbRpcClient | None", kwargs.pop("db_client", None))
        raw_refresh = kwargs.pop("refresh_interval", None)
        self._refresh_interval = _DEFAULT_REFRESH_SECONDS
        if raw_refresh is not None:
            try:
                self._refresh_interval = float(cast("float | int | str", raw_refresh))
            except (TypeError, ValueError):
                self._logger.warning("Invalid refresh_interval value: %r", raw_refresh)
        self._last_refresh_at = 0.0
        self._schedule_cache: dict[str, Schedule] = {}
        self._run_state_cache: dict[str, tuple[datetime | None, int | None]] = {}
        self._app_label: str | None = None
        super().__init__(*args, **kwargs)
        conf_value = self.app.conf.get(_REFRESH_CONF_KEY)
        if conf_value is not None:
            try:
                self._refresh_interval = float(conf_value)
            except (TypeError, ValueError):
                self._logger.warning("Invalid %s value: %r", _REFRESH_CONF_KEY, conf_value)

    def setup_schedule(self) -> None:
        """Populate the in-memory schedule from the DB."""
        self._app_label = _resolve_app_name(self.app)
        self._refresh(force=True)

    def tick(self) -> float:
        """Run one tick, refreshing schedules and writing run state as needed."""
        self._refresh()
        delay = cast("Any", super()).tick()
        self._writeback_run_state()
        return delay

    def sync(self) -> None:
        """Flush run state updates to the DB."""
        self._writeback_run_state()

    def close(self) -> None:
        """Flush run state updates and close DB resources."""
        self._writeback_run_state()
        if self._db_client is not None:
            self._db_client.close()
        super().close()

    def _refresh(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if not force and (now - self._last_refresh_at) < self._refresh_interval:
            return
        self._last_refresh_at = now
        try:
            schedules = self._fetch_schedules()
        except (OSError, RuntimeError, ValueError) as exc:  # pragma: no cover - network dependent
            self._logger.warning("Beat DB refresh failed: %s", exc)
            return
        self._load_schedules(schedules)

    def _fetch_schedules(self) -> list[Schedule]:
        db = self._get_db()
        schedules = list(db.get_schedules())
        label = self._app_label or _resolve_app_name(self.app)
        return [schedule for schedule in schedules if schedule.app == label]

    def _load_schedules(self, schedules: Iterable[Schedule]) -> None:
        schedule_map: dict[str, _EntryLike] = {}
        cache: dict[str, Schedule] = {}
        run_cache: dict[str, tuple[datetime | None, int | None]] = {}
        for schedule in schedules:
            if not schedule.enabled:
                continue
            entry = self._build_entry(schedule)
            schedule_map[schedule.schedule_id] = entry
            cache[schedule.schedule_id] = schedule
            run_cache[schedule.schedule_id] = (entry.last_run_at, entry.total_run_count)
        self._schedule_cache = cache
        self._run_state_cache = run_cache
        self.schedule = cast("dict[str, ScheduleEntry]", schedule_map)
        self._heap = None

    def _build_entry(self, schedule: Schedule) -> _EntryLike:
        schedule_obj = _parse_schedule(schedule.schedule)
        args = _parse_args(schedule.args)
        kwargs = _parse_kwargs(schedule.kwargs_)
        last_run_at = schedule.last_run_at or self.app.now()
        total_run_count = int(schedule.total_run_count or 0)
        entry_cls = cast("Any", ScheduleEntry)
        entry = entry_cls(
            name=schedule.schedule_id,
            task=schedule.task,
            schedule=schedule_obj,
            args=args,
            kwargs=kwargs,
            options={"enabled": schedule.enabled},
            last_run_at=last_run_at,
            total_run_count=total_run_count,
            app=self.app,
        )
        return cast("_EntryLike", entry)

    def _writeback_run_state(self) -> None:
        if not self._schedule_cache:
            return
        dirty: list[Schedule] = []
        for schedule_id, schedule in self._schedule_cache.items():
            entry = self.schedule.get(schedule_id)
            if entry is None:
                continue
            entry_like = cast("_EntryLike", entry)
            cached = self._run_state_cache.get(schedule_id)
            current = (entry_like.last_run_at, entry_like.total_run_count)
            if cached == current:
                continue
            schedule.last_run_at = entry_like.last_run_at
            schedule.total_run_count = int(entry_like.total_run_count or 0)
            dirty.append(schedule)
            self._run_state_cache[schedule_id] = current
        if not dirty:
            return
        db = self._get_db()
        for schedule in dirty:
            try:
                db.store_schedule(schedule)
            except (OSError, RuntimeError, ValueError) as exc:  # pragma: no cover - network dependent
                self._logger.warning("Failed to persist beat run state for %s: %s", schedule.schedule_id, exc)

    def _get_db(self) -> DbRpcClient:
        if self._db_client is None:
            config = get_settings()
            self._db_client = DbRpcClient.from_config(config, client_name="beat")
        return self._db_client


__all__ = ["DatabaseScheduler"]
