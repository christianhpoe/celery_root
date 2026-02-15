# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime

from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.models import TaskEvent, TaskFilter, TimeRange


def test_get_tasks_page_and_names() -> None:
    controller = SQLiteController()
    controller.initialize()
    controller.ensure_schema()

    now = datetime.now(UTC)
    controller.store_task_event(TaskEvent(task_id="t1", name="demo.add", state="SUCCESS", timestamp=now, worker="w1"))
    controller.store_task_event(TaskEvent(task_id="t2", name="demo.mul", state="FAILURE", timestamp=now, worker="w2"))

    names = controller.list_task_names()
    assert "demo.add" in names

    filters = TaskFilter(search="demo")
    tasks, total = controller.get_tasks_page(filters, sort_key="state", sort_dir="desc", limit=10, offset=0)
    assert total == 2
    assert tasks

    time_range = TimeRange(start=now, end=now)
    filters = TaskFilter(time_range=time_range)
    tasks, total = controller.get_tasks_page(filters, sort_key="runtime", sort_dir="asc", limit=1, offset=0)
    assert total == 2

    heatmap = controller.get_heatmap(None)
    assert len(heatmap) == 7

    controller.close()
