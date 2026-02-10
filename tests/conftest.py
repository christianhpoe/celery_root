from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import django
import pytest
from django.test import Client

from celery_cnc.config import reset_settings
from celery_cnc.db.models import Schedule, TaskEvent, WorkerEvent
from celery_cnc.db.sqlite import SQLiteController
from tests.fixtures.app_one import app as app_one
from tests.fixtures.app_two import app as app_two

if TYPE_CHECKING:
    from pathlib import Path

    from celery import Celery


@pytest.fixture
def celery_app_one() -> Celery:
    return app_one


@pytest.fixture
def celery_app_two() -> Celery:
    return app_two


@pytest.fixture(scope="session")
def web_client(tmp_path_factory: pytest.TempPathFactory) -> Client:
    db_path = tmp_path_factory.mktemp("celery_cnc") / "celery_cnc.db"
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_cnc.web.settings")
    os.environ["CELERY_CNC_DB_PATH"] = str(db_path)
    reset_settings()

    if not django.apps.apps.ready:
        django.setup()

    _seed_db(db_path)
    return Client()


def _seed_db(path: Path) -> None:
    db = SQLiteController(path)
    db.initialize()
    now = datetime.now(UTC)
    db.store_task_event(
        TaskEvent(
            task_id="task-0001",
            name="demo.task",
            state="SUCCESS",
            timestamp=now,
            worker="alpha",
            args="[]",
            kwargs="{}",
            result="ok",
        ),
    )
    db.store_worker_event(
        WorkerEvent(
            hostname="alpha",
            event="worker-online",
            timestamp=now,
            info={"pool": {"max-concurrency": 4}, "active": []},
        ),
    )
    db.store_schedule(
        Schedule(
            schedule_id="sched-1",
            name="demo schedule",
            task="demo.task",
            schedule="*/5 * * * *",
            args="[]",
            kwargs="{}",
            enabled=True,
            last_run_at=now,
            total_run_count=1,
        ),
    )
    db.close()
