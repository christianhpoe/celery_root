# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from celery_root.core.db.adapters.sqlite import SQLiteController, _merge_retries
from celery_root.core.db.models import TaskEvent, WorkerEvent

if TYPE_CHECKING:
    from pathlib import Path


def test_merge_retries() -> None:
    assert _merge_retries(None, 2) == 2
    assert _merge_retries(1, None) == 1
    assert _merge_retries(1, 3) == 3


def test_worker_parsing_helpers() -> None:
    assert SQLiteController._parse_active_tasks([1, 2]) == 2
    assert SQLiteController._parse_active_tasks(3) == 3
    assert SQLiteController._parse_pool_size({"max-concurrency": 4}) == 4
    assert SQLiteController._parse_registered_tasks(["a", "b"]) == ["a", "b"]
    assert SQLiteController._parse_queue_names([{"name": "celery"}, "other"]) == ["celery", "other"]


def test_cleanup_removes_old_records(tmp_path: Path) -> None:
    controller = SQLiteController(tmp_path / "cleanup.db")
    controller.initialize()
    controller.ensure_schema()
    old_ts = datetime.now(UTC) - timedelta(days=10)
    controller.store_task_event(TaskEvent(task_id="t1", name="demo", state="SUCCESS", timestamp=old_ts))
    controller.store_worker_event(WorkerEvent(hostname="w1", event="worker-online", timestamp=old_ts))
    removed = controller.cleanup(older_than_days=1)
    assert removed >= 1
    controller.close()
