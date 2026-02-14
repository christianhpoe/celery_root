# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, get_settings
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.manager import DBManager
from celery_root.core.db.rpc_client import DbRpcClient
from celery_root.core.event_listener import EventListener
from tests.fixtures.app_one import app as app_one
from tests.fixtures.app_two import app as app_two

pytestmark = pytest.mark.skipif(
    not get_settings().integration,
    reason="integration test requires running brokers and celery workers",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


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
        db_path = tmp_path / "integration.sqlite3"
        rpc_port = _free_port()
        config = CeleryRootConfig(
            database=DatabaseConfigSqlite(
                batch_size=1,
                flush_interval=0.2,
                db_path=db_path,
                rpc_port=rpc_port,
            ),
        )

        def factory() -> SQLiteController:
            return SQLiteController(db_path)

        db_manager = DBManager(config, factory)
        db_manager.start()
        _wait_for_db_manager(config)

        listeners = [
            EventListener(str(app_one.conf.broker_url), config),
            EventListener(str(app_two.conf.broker_url), config),
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
