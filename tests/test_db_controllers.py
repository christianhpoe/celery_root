from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from celery_cnc.db.memory import MemoryController
from celery_cnc.db.models import (
    Schedule,
    TaskEvent,
    TaskFilter,
    TaskRelation,
    TimeRange,
    WorkerEvent,
)
from celery_cnc.db.sqlite import SQLiteController

if TYPE_CHECKING:
    from collections.abc import Generator
    from pathlib import Path

    from celery_cnc.db.abc import BaseDBController


@pytest.fixture(params=["memory", "sqlite"])
def controller(request: pytest.FixtureRequest, tmp_path: Path) -> Generator[BaseDBController]:
    if request.param == "memory":
        ctrl: BaseDBController = MemoryController()
    else:
        ctrl = SQLiteController(tmp_path / "sqlite3.db")
    ctrl.initialize()
    yield ctrl
    ctrl.close()


def _task_event(  # noqa: PLR0913
    task_id: str,
    state: str,
    timestamp: datetime,
    *,
    name: str = "tests.add",
    worker: str | None = None,
    args: str | None = None,
    kwargs: str | None = None,
    result: str | None = None,
    traceback: str | None = None,
    runtime: float | None = None,
    retries: int | None = None,
    eta: datetime | None = None,
    expires: datetime | None = None,
    parent_id: str | None = None,
    root_id: str | None = None,
    group_id: str | None = None,
    chord_id: str | None = None,
) -> TaskEvent:
    return TaskEvent(
        task_id=task_id,
        name=name,
        state=state,
        timestamp=timestamp,
        worker=worker,
        args=args,
        kwargs=kwargs,
        result=result,
        traceback=traceback,
        runtime=runtime,
        retries=retries,
        eta=eta,
        expires=expires,
        parent_id=parent_id,
        root_id=root_id,
        group_id=group_id,
        chord_id=chord_id,
    )


def test_store_and_get_task(controller: BaseDBController) -> None:
    ts = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
    controller.store_task_event(_task_event("t1", "RECEIVED", ts, worker="w1"))
    controller.store_task_event(_task_event("t1", "STARTED", ts + timedelta(seconds=1)))
    controller.store_task_event(_task_event("t1", "SUCCESS", ts + timedelta(seconds=2), runtime=0.5, result="ok"))

    task = controller.get_task("t1")
    assert task is not None
    assert task.state == "SUCCESS"
    assert task.worker == "w1"
    assert task.runtime == 0.5
    assert task.finished is not None


def test_filters_and_search(controller: BaseDBController) -> None:
    base = datetime(2024, 1, 2, 9, 0, 0, tzinfo=UTC)
    controller.store_task_event(_task_event("t1", "SUCCESS", base, name="tests.add", args="1,2", worker="w1"))
    controller.store_task_event(_task_event("t2", "FAILURE", base + timedelta(minutes=1), name="tests.mul", args="3,4"))
    controller.store_task_event(
        _task_event(
            "t3",
            "SUCCESS",
            base + timedelta(minutes=2),
            name="tests.add",
            kwargs='{"x": 1}',
        ),
    )

    filters = TaskFilter(task_name="tests.add", state="SUCCESS")
    tasks = controller.get_tasks(filters)
    assert {task.task_id for task in tasks} == {"t1", "t3"}

    search = TaskFilter(search="x")
    tasks = controller.get_tasks(search)
    assert {task.task_id for task in tasks} == {"t3"}


def test_relations(controller: BaseDBController) -> None:
    relation = TaskRelation(root_id="root", parent_id=None, child_id="child", relation="chain")
    controller.store_task_relation(relation)
    rels = controller.get_task_relations("root")
    assert len(rels) == 1
    assert rels[0].child_id == "child"


def test_workers(controller: BaseDBController) -> None:
    ts = datetime(2024, 1, 3, 8, 0, 0, tzinfo=UTC)
    controller.store_worker_event(
        WorkerEvent(hostname="worker1", event="worker-online", timestamp=ts, info={"active": []}),
    )
    worker = controller.get_worker("worker1")
    assert worker is not None
    assert worker.status == "ONLINE"


def test_schedules(controller: BaseDBController) -> None:
    schedule = Schedule(
        schedule_id="sched1",
        name="daily",
        task="tests.add",
        schedule="crontab",
        args="[1, 2]",
    )
    controller.store_schedule(schedule)
    schedules = controller.get_schedules()
    assert len(schedules) == 1
    controller.delete_schedule("sched1")
    assert controller.get_schedules() == []


def test_stats_and_distribution(controller: BaseDBController) -> None:
    base = datetime(2024, 1, 4, 10, 0, 0, tzinfo=UTC)
    controller.store_task_event(_task_event("t1", "SUCCESS", base, runtime=1.0))
    controller.store_task_event(_task_event("t2", "SUCCESS", base, runtime=2.0))
    controller.store_task_event(_task_event("t3", "FAILURE", base, runtime=3.0))

    stats = controller.get_task_stats(None, None)
    assert stats.count == 3
    assert stats.min_runtime == 1.0
    assert stats.max_runtime == 3.0

    distribution = controller.get_state_distribution()
    assert distribution["SUCCESS"] == 2
    assert distribution["FAILURE"] == 1


def test_throughput_and_heatmap(controller: BaseDBController) -> None:
    base = datetime(2024, 1, 5, 0, 0, 0, tzinfo=UTC)
    controller.store_task_event(_task_event("t1", "SUCCESS", base))
    controller.store_task_event(_task_event("t2", "SUCCESS", base + timedelta(seconds=30)))
    controller.store_task_event(_task_event("t3", "SUCCESS", base + timedelta(minutes=1)))

    time_range = TimeRange(start=base, end=base + timedelta(minutes=2))
    buckets = controller.get_throughput(time_range, bucket_seconds=60)
    counts = [bucket.count for bucket in buckets]
    assert counts[0] == 2
    assert counts[1] == 1

    heatmap = controller.get_heatmap(time_range)
    assert heatmap[base.weekday()][base.hour] >= 3


def test_cleanup(controller: BaseDBController) -> None:
    old = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
    controller.store_task_event(_task_event("t1", "SUCCESS", old))
    removed = controller.cleanup(older_than_days=1)
    assert removed >= 1
