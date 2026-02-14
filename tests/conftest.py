# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import os
import secrets
import socket
import time
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
    get_settings,
    reset_settings,
    set_settings,
)
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.manager import DBManager
from celery_root.core.db.models import Schedule, TaskEvent, WorkerEvent
from celery_root.core.db.rpc_client import DbRpcClient
from tests.fixtures.app_one import app as app_one
from tests.fixtures.app_two import app as app_two

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from celery import Celery


@pytest.fixture
def celery_app_one() -> Celery:
    return app_one


@pytest.fixture
def celery_app_two() -> Celery:
    return app_two


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _wait_for_db_manager(config: CeleryRootConfig, timeout_seconds: float = 5.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    client = DbRpcClient.from_config(config, client_name="tests")
    try:
        while time.monotonic() < deadline:
            try:
                client.ping()
            except (OSError, RuntimeError):
                time.sleep(0.05)
            else:
                return
        msg = "DB manager did not become ready in time"
        raise RuntimeError(msg)
    finally:
        client.close()


@pytest.fixture(scope="session")
def web_client(tmp_path_factory: pytest.TempPathFactory) -> Generator[Client, None, None]:
    db_path = tmp_path_factory.mktemp("celery_root") / "celery_root.db"
    log_dir = tmp_path_factory.mktemp("celery_root_logs")
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    reset_settings()
    rpc_port = _free_port()
    rpc_auth_key = secrets.token_urlsafe(32)
    set_settings(
        CeleryRootConfig(
            logging=LoggingConfigFile(log_dir=log_dir),
            database=DatabaseConfigSqlite(db_path=db_path, rpc_port=rpc_port, rpc_auth_key=rpc_auth_key),
            beat=BeatConfig(),
            frontend=FrontendConfig(secret_key=secrets.token_urlsafe(32), debug=False),
        ),
    )

    if not django.apps.apps.ready:
        django.setup()

    _seed_db(db_path)
    config = get_settings()
    manager = DBManager(config)
    manager.start()
    _wait_for_db_manager(config)
    try:
        yield Client()
    finally:
        manager.stop()
        manager.join(timeout=5)
        if manager.is_alive():
            manager.terminate()
            manager.join(timeout=5)


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
            kwargs_="{}",
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
            kwargs_="{}",
            enabled=True,
            last_run_at=now,
            total_run_count=1,
        ),
    )
    db.close()
