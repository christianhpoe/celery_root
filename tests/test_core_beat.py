# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from celery import Celery
from celery.schedules import crontab

from celery_root.components.beat import BeatController
from celery_root.core.db.models import Schedule

if TYPE_CHECKING:
    from pathlib import Path


def _make_app(schedule_filename: Path) -> Celery:
    app = Celery("beat-tests", broker="memory://", backend="cache+memory://")
    app.conf.beat_schedule_filename = str(schedule_filename)
    app.conf.beat_schedule = {
        "nightly": {
            "task": "tasks.cleanup",
            "schedule": crontab(minute=0, hour=2),
            "args": (),
            "kwargs": {},
        },
    }
    return app


def test_file_backend_list_save_delete(tmp_path: Path) -> None:
    app = _make_app(tmp_path / "celerybeat-schedule")
    controller = BeatController(app)

    schedules = controller.list_schedules()
    assert any(entry.name == "nightly" for entry in schedules)

    new_schedule = Schedule(
        schedule_id="heartbeat",
        name="heartbeat",
        task="tasks.heartbeat",
        schedule="interval:30",
        args="[]",
        kwargs_="{}",
        enabled=True,
        last_run_at=datetime.now(UTC),
        total_run_count=0,
    )
    controller.save_schedule(new_schedule)

    updated = controller.list_schedules()
    assert any(entry.name == "heartbeat" for entry in updated)

    controller.delete_schedule("heartbeat")
    final = controller.list_schedules()
    assert not any(entry.name == "heartbeat" for entry in final)
