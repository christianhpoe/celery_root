# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import multiprocessing
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, get_settings
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.manager import DBManager
from celery_root.core.event_listener import EventListener
from tests.fixtures.app_one import app as app_one
from tests.fixtures.app_two import app as app_two

pytestmark = pytest.mark.skipif(
    not get_settings().integration,
    reason="integration test requires running brokers and celery workers",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _start_worker(module: str) -> subprocess.Popen[str]:
    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        module,
        "worker",
        "-E",
        "--pool=solo",
        "--concurrency=1",
        "--loglevel=INFO",
    ]
    return subprocess.Popen(  # noqa: S603
        cmd,
        cwd=_repo_root(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def test_integration_event_pipeline(tmp_path: Path) -> None:
    workers = [
        _start_worker("tests.fixtures.app_one"),
        _start_worker("tests.fixtures.app_two"),
    ]
    listeners: list[EventListener] = []
    db_manager: DBManager | None = None
    try:
        queue: multiprocessing.Queue[object] = multiprocessing.Queue()
        config = CeleryRootConfig(database=DatabaseConfigSqlite(batch_size=1, flush_interval=0.2))
        db_path = tmp_path / "integration.sqlite3"

        def factory() -> SQLiteController:
            return SQLiteController(db_path)

        db_manager = DBManager(queue, factory, config)
        db_manager.start()

        listeners = [
            EventListener(str(app_one.conf.broker_url), queue),
            EventListener(str(app_two.conf.broker_url), queue),
        ]
        for listener in listeners:
            listener.start()

        app_one.send_task("fixture.add", args=(1, 2))
        app_two.send_task("fixture.mul", args=(3, 4))

        db_reader = SQLiteController(db_path)
        db_reader.initialize()
        deadline = time.time() + 15
        while time.time() < deadline:
            tasks = db_reader.get_tasks()
            if len(tasks) >= 2:
                break
            time.sleep(0.5)

        tasks = db_reader.get_tasks()
        assert len(tasks) >= 2
        db_reader.close()
    finally:
        for listener in listeners:
            listener.stop()
        if db_manager is not None:
            db_manager.stop()
            db_manager.join(timeout=5)
        for listener in listeners:
            listener.join(timeout=5)
        for worker in workers:
            worker.send_signal(signal.SIGTERM)
            try:
                worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
