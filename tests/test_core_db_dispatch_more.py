# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime

from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.dispatch import RPC_OPERATIONS
from celery_root.shared.schemas import (
    BrokerQueueSnapshotRequest,
    GetTaskRequest,
    IngestBrokerQueueEventRequest,
    IngestTaskEventRequest,
    IngestWorkerEventRequest,
    ListTaskNamesRequest,
    ListTasksPageRequest,
    ListTasksRequest,
    StoreTaskRelationRequest,
    WorkerEventSnapshotRequest,
)
from celery_root.shared.schemas.domain import BrokerQueueEvent, TaskEvent, TaskRelation, WorkerEvent


def test_dispatch_operations() -> None:
    controller = SQLiteController()
    controller.initialize()
    controller.ensure_schema()

    now = datetime.now(UTC)
    task_event = TaskEvent(task_id="t1", name="demo", state="SUCCESS", timestamp=now)
    worker_event = WorkerEvent(hostname="w1", event="worker-online", timestamp=now, info={"active": []})
    broker_event = BrokerQueueEvent(broker_url="redis://", queue="celery", messages=1, consumers=1, timestamp=now)

    RPC_OPERATIONS["events.task.ingest"].handler(controller, IngestTaskEventRequest(event=task_event))
    RPC_OPERATIONS["events.worker.ingest"].handler(controller, IngestWorkerEventRequest(event=worker_event))
    RPC_OPERATIONS["events.broker_queue.ingest"].handler(controller, IngestBrokerQueueEventRequest(event=broker_event))

    tasks = RPC_OPERATIONS["tasks.list"].handler(controller, ListTasksRequest()).tasks
    assert tasks

    page = RPC_OPERATIONS["tasks.page"].handler(
        controller,
        ListTasksPageRequest(filters=None, sort_key=None, sort_dir=None, limit=10, offset=0),
    )
    assert page.total >= 1

    names = RPC_OPERATIONS["tasks.names"].handler(controller, ListTaskNamesRequest()).names
    assert "demo" in names

    task = RPC_OPERATIONS["tasks.get"].handler(controller, GetTaskRequest(task_id="t1")).task
    assert task is not None

    relations = RPC_OPERATIONS["relations.store"].handler(
        controller,
        StoreTaskRelationRequest(relation=TaskRelation(root_id="t1", parent_id=None, child_id="t2", relation="chain")),
    )
    assert relations.ok

    snapshots = RPC_OPERATIONS["broker.queues.snapshot"].handler(
        controller,
        BrokerQueueSnapshotRequest(broker_url="redis://"),
    )
    assert snapshots.events

    worker_snapshot = RPC_OPERATIONS["workers.events.snapshot"].handler(
        controller,
        WorkerEventSnapshotRequest(hostname="w1"),
    )
    assert worker_snapshot.event is not None

    controller.close()
