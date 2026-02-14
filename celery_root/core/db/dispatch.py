# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""RPC operation registry for DB manager."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel
from sqlalchemy import inspect

from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.shared.schemas import (
    CleanupRequest,
    CleanupResponse,
    DeleteScheduleRequest,
    GetTaskRequest,
    GetTaskResponse,
    GetWorkerRequest,
    GetWorkerResponse,
    HeatmapRequest,
    HeatmapResponse,
    IngestTaskEventRequest,
    IngestWorkerEventRequest,
    ListSchedulesRequest,
    ListSchedulesResponse,
    ListTaskNamesRequest,
    ListTaskNamesResponse,
    ListTaskRelationsRequest,
    ListTaskRelationsResponse,
    ListTasksPageRequest,
    ListTasksPageResponse,
    ListTasksRequest,
    ListTasksResponse,
    ListWorkersRequest,
    ListWorkersResponse,
    Ok,
    PingRequest,
    PingResponse,
    SchemaColumn,
    SchemaIndex,
    SchemaRequest,
    SchemaResponse,
    SchemaTable,
    SchemaVersionRequest,
    SchemaVersionResponse,
    StateDistributionRequest,
    StateDistributionResponse,
    StoreScheduleRequest,
    StoreTaskRelationRequest,
    TaskStatsRequest,
    TaskStatsResponse,
    ThroughputRequest,
    ThroughputResponse,
)
from celery_root.shared.schemas.domain import TaskEvent, TaskRelation

if TYPE_CHECKING:
    from collections.abc import Callable

    from celery_root.core.db.adapters.base import BaseDBController


@dataclass(frozen=True, slots=True)
class RpcOperation[ReqT: BaseModel, ResT: BaseModel]:
    """RPC operation specification."""

    op: str
    request_model: type[ReqT]
    response_model: type[ResT]
    handler: Callable[[BaseDBController, ReqT], ResT]


def _store_relations(controller: BaseDBController, event: TaskEvent) -> None:
    root_id = event.root_id or event.task_id
    if event.parent_id:
        controller.store_task_relation(
            TaskRelation(
                root_id=root_id,
                parent_id=event.parent_id,
                child_id=event.task_id,
                relation="parent",
            ),
        )
    if event.group_id:
        controller.store_task_relation(
            TaskRelation(
                root_id=root_id,
                parent_id=event.group_id,
                child_id=event.task_id,
                relation="group",
            ),
        )
    if event.chord_id:
        controller.store_task_relation(
            TaskRelation(
                root_id=root_id,
                parent_id=event.chord_id,
                child_id=event.task_id,
                relation="chord",
            ),
        )


def _ping(_controller: BaseDBController, _request: PingRequest) -> PingResponse:
    return PingResponse()


def _schema_version(controller: BaseDBController, _request: SchemaVersionRequest) -> SchemaVersionResponse:
    return SchemaVersionResponse(version=controller.get_schema_version())


def _schema(controller: BaseDBController, _request: SchemaRequest) -> SchemaResponse:
    if not isinstance(controller, SQLiteController):
        msg = "Schema introspection is only supported for SQLite backends."
        raise TypeError(msg)
    inspector = inspect(controller._engine)  # noqa: SLF001
    tables: dict[str, SchemaTable] = {}
    for table in inspector.get_table_names():
        columns = inspector.get_columns(table)
        indexes = inspector.get_indexes(table)
        tables[table] = SchemaTable(
            columns=[
                SchemaColumn(
                    name=column["name"],
                    type=str(column["type"]),
                    nullable=bool(column.get("nullable", True)),
                    default=str(column["default"]) if column.get("default") is not None else None,
                    primary_key=bool(column.get("primary_key", False)),
                )
                for column in columns
            ],
            indexes=[
                SchemaIndex(
                    name=index.get("name"),
                    column_names=list(index.get("column_names", []) or []),
                    unique=bool(index.get("unique")) if index.get("unique") is not None else None,
                )
                for index in indexes
            ],
        )
    return SchemaResponse(dialect=controller._engine.dialect.name, tables=tables)  # noqa: SLF001


def _ingest_task_event(controller: BaseDBController, request: IngestTaskEventRequest) -> Ok:
    controller.store_task_event(request.event)
    _store_relations(controller, request.event)
    return Ok()


def _ingest_worker_event(controller: BaseDBController, request: IngestWorkerEventRequest) -> Ok:
    controller.store_worker_event(request.event)
    return Ok()


def _store_relation(controller: BaseDBController, request: StoreTaskRelationRequest) -> Ok:
    controller.store_task_relation(request.relation)
    return Ok()


def _list_tasks(controller: BaseDBController, request: ListTasksRequest) -> ListTasksResponse:
    tasks = list(controller.get_tasks(request.filters))
    return ListTasksResponse(tasks=tasks)


def _list_tasks_page(controller: BaseDBController, request: ListTasksPageRequest) -> ListTasksPageResponse:
    tasks, total = controller.get_tasks_page(
        request.filters,
        sort_key=request.sort_key,
        sort_dir=request.sort_dir,
        limit=request.limit,
        offset=request.offset,
    )
    return ListTasksPageResponse(tasks=list(tasks), total=total)


def _list_task_names(controller: BaseDBController, _request: ListTaskNamesRequest) -> ListTaskNamesResponse:
    names = list(controller.list_task_names())
    return ListTaskNamesResponse(names=names)


def _get_task(controller: BaseDBController, request: GetTaskRequest) -> GetTaskResponse:
    task = controller.get_task(request.task_id)
    return GetTaskResponse(task=task)


def _list_relations(controller: BaseDBController, request: ListTaskRelationsRequest) -> ListTaskRelationsResponse:
    relations = list(controller.get_task_relations(request.root_id))
    return ListTaskRelationsResponse(relations=relations)


def _list_workers(controller: BaseDBController, _request: ListWorkersRequest) -> ListWorkersResponse:
    workers = list(controller.get_workers())
    return ListWorkersResponse(workers=workers)


def _get_worker(controller: BaseDBController, request: GetWorkerRequest) -> GetWorkerResponse:
    worker = controller.get_worker(request.hostname)
    return GetWorkerResponse(worker=worker)


def _task_stats(controller: BaseDBController, request: TaskStatsRequest) -> TaskStatsResponse:
    stats = controller.get_task_stats(request.task_name, request.time_range)
    return TaskStatsResponse(stats=stats)


def _throughput(controller: BaseDBController, request: ThroughputRequest) -> ThroughputResponse:
    buckets = list(controller.get_throughput(request.time_range, request.bucket_seconds))
    return ThroughputResponse(buckets=buckets)


def _state_distribution(controller: BaseDBController, _request: StateDistributionRequest) -> StateDistributionResponse:
    counts = controller.get_state_distribution()
    return StateDistributionResponse(counts=counts)


def _heatmap(controller: BaseDBController, request: HeatmapRequest) -> HeatmapResponse:
    heatmap = controller.get_heatmap(request.time_range)
    return HeatmapResponse(heatmap=heatmap)


def _list_schedules(controller: BaseDBController, _request: ListSchedulesRequest) -> ListSchedulesResponse:
    schedules = list(controller.get_schedules())
    return ListSchedulesResponse(schedules=schedules)


def _store_schedule(controller: BaseDBController, request: StoreScheduleRequest) -> Ok:
    controller.store_schedule(request.schedule)
    return Ok()


def _delete_schedule(controller: BaseDBController, request: DeleteScheduleRequest) -> Ok:
    controller.delete_schedule(request.schedule_id)
    return Ok()


def _cleanup(controller: BaseDBController, request: CleanupRequest) -> CleanupResponse:
    removed = controller.cleanup(request.older_than_days)
    return CleanupResponse(removed=removed)


RPC_OPERATIONS: dict[str, RpcOperation[Any, Any]] = {
    "db.ping": RpcOperation("db.ping", PingRequest, PingResponse, _ping),
    "db.schema_version": RpcOperation(
        "db.schema_version",
        SchemaVersionRequest,
        SchemaVersionResponse,
        _schema_version,
    ),
    "db.schema": RpcOperation(
        "db.schema",
        SchemaRequest,
        SchemaResponse,
        _schema,
    ),
    "events.task.ingest": RpcOperation(
        "events.task.ingest",
        IngestTaskEventRequest,
        Ok,
        _ingest_task_event,
    ),
    "events.worker.ingest": RpcOperation(
        "events.worker.ingest",
        IngestWorkerEventRequest,
        Ok,
        _ingest_worker_event,
    ),
    "relations.store": RpcOperation(
        "relations.store",
        StoreTaskRelationRequest,
        Ok,
        _store_relation,
    ),
    "tasks.list": RpcOperation("tasks.list", ListTasksRequest, ListTasksResponse, _list_tasks),
    "tasks.page": RpcOperation(
        "tasks.page",
        ListTasksPageRequest,
        ListTasksPageResponse,
        _list_tasks_page,
    ),
    "tasks.names": RpcOperation(
        "tasks.names",
        ListTaskNamesRequest,
        ListTaskNamesResponse,
        _list_task_names,
    ),
    "tasks.get": RpcOperation("tasks.get", GetTaskRequest, GetTaskResponse, _get_task),
    "relations.list": RpcOperation(
        "relations.list",
        ListTaskRelationsRequest,
        ListTaskRelationsResponse,
        _list_relations,
    ),
    "workers.list": RpcOperation(
        "workers.list",
        ListWorkersRequest,
        ListWorkersResponse,
        _list_workers,
    ),
    "workers.get": RpcOperation("workers.get", GetWorkerRequest, GetWorkerResponse, _get_worker),
    "stats.task": RpcOperation("stats.task", TaskStatsRequest, TaskStatsResponse, _task_stats),
    "stats.throughput": RpcOperation(
        "stats.throughput",
        ThroughputRequest,
        ThroughputResponse,
        _throughput,
    ),
    "stats.state_distribution": RpcOperation(
        "stats.state_distribution",
        StateDistributionRequest,
        StateDistributionResponse,
        _state_distribution,
    ),
    "stats.heatmap": RpcOperation("stats.heatmap", HeatmapRequest, HeatmapResponse, _heatmap),
    "schedules.list": RpcOperation(
        "schedules.list",
        ListSchedulesRequest,
        ListSchedulesResponse,
        _list_schedules,
    ),
    "schedules.store": RpcOperation(
        "schedules.store",
        StoreScheduleRequest,
        Ok,
        _store_schedule,
    ),
    "schedules.delete": RpcOperation(
        "schedules.delete",
        DeleteScheduleRequest,
        Ok,
        _delete_schedule,
    ),
    "db.cleanup": RpcOperation("db.cleanup", CleanupRequest, CleanupResponse, _cleanup),
}
