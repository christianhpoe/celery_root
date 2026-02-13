# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import os
import secrets
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import django
import pytest
from django.test import Client

from celery_root.config import (
    BeatConfig,
    CeleryRootConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    LoggingConfigFile,
    reset_settings,
    set_settings,
)
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.models import Schedule, TaskEvent, WorkerEvent
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
    db_path = tmp_path_factory.mktemp("celery_root") / "celery_root.db"
    log_dir = tmp_path_factory.mktemp("celery_root_logs")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    reset_settings()
    set_settings(
        CeleryRootConfig(
            logging=LoggingConfigFile(log_dir=log_dir),
            database=DatabaseConfigSqlite(db_path=db_path),
            beat=BeatConfig(),
            frontend=FrontendConfig(secret_key=secrets.token_urlsafe(32), debug=False),
        ),
    )

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
