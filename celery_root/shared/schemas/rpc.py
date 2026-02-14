# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""RPC envelope and operation schemas for DB manager communication."""

from __future__ import annotations

import datetime as _dt
import importlib
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict

if TYPE_CHECKING:
    from .domain import (
        Schedule,
        Task,
        TaskEvent,
        TaskFilter,
        TaskRelation,
        TaskStats,
        ThroughputBucket,
        TimeRange,
        Worker,
        WorkerEvent,
    )
else:
    _domain = importlib.import_module("celery_root.shared.schemas.domain")
    Schedule = _domain.Schedule
    Task = _domain.Task
    TaskEvent = _domain.TaskEvent
    TaskFilter = _domain.TaskFilter
    TaskRelation = _domain.TaskRelation
    TaskStats = _domain.TaskStats
    ThroughputBucket = _domain.ThroughputBucket
    TimeRange = _domain.TimeRange
    Worker = _domain.Worker
    WorkerEvent = _domain.WorkerEvent

Datetime = _dt.datetime

RPC_SCHEMA_VERSION = 1


class _BaseSchema(BaseModel):
    """Base schema with shared configuration."""

    model_config = ConfigDict(extra="ignore", validate_assignment=False)


class RpcError(_BaseSchema):
    """Structured error returned by the RPC protocol."""

    code: str
    message: str
    details: dict[str, Any] | None = None


class RpcRequestEnvelope(_BaseSchema):
    """Envelope for all RPC requests."""

    request_id: str
    op: str
    payload: dict[str, Any] | list[Any] | None = None
    schema_version: int = RPC_SCHEMA_VERSION
    timestamp: Datetime | None = None
    client: str | None = None
    trace: dict[str, str] | None = None


class RpcResponseEnvelope(_BaseSchema):
    """Envelope for all RPC responses."""

    request_id: str
    ok: bool
    payload: dict[str, Any] | list[Any] | None = None
    error: RpcError | None = None
    schema_version: int = RPC_SCHEMA_VERSION
    timestamp: Datetime | None = None


class Ok(_BaseSchema):
    """Generic success payload."""

    ok: bool = True


class PingRequest(_BaseSchema):
    """Health check request."""


class PingResponse(_BaseSchema):
    """Health check response."""

    status: Literal["ok"] = "ok"


class IngestTaskEventRequest(_BaseSchema):
    """Request to ingest a task event."""

    event: TaskEvent
    idempotency_key: str | None = None


class IngestWorkerEventRequest(_BaseSchema):
    """Request to ingest a worker event."""

    event: WorkerEvent
    idempotency_key: str | None = None


class StoreTaskRelationRequest(_BaseSchema):
    """Request to persist a task relation."""

    relation: TaskRelation
    idempotency_key: str | None = None


class ListTasksRequest(_BaseSchema):
    """Request to list tasks."""

    filters: TaskFilter | None = None


class ListTasksResponse(_BaseSchema):
    """Response with matching tasks."""

    tasks: list[Task]


class ListTasksPageRequest(_BaseSchema):
    """Request for a paginated task list."""

    filters: TaskFilter | None = None
    sort_key: str | None = None
    sort_dir: str | None = None
    limit: int
    offset: int


class ListTasksPageResponse(_BaseSchema):
    """Response with paginated tasks and total count."""

    tasks: list[Task]
    total: int


class ListTaskNamesRequest(_BaseSchema):
    """Request to list distinct task names."""


class ListTaskNamesResponse(_BaseSchema):
    """Response with distinct task names."""

    names: list[str]


class GetTaskRequest(_BaseSchema):
    """Request to fetch a single task."""

    task_id: str


class GetTaskResponse(_BaseSchema):
    """Response with a single task."""

    task: Task | None


class ListTaskRelationsRequest(_BaseSchema):
    """Request to list relations for a root task."""

    root_id: str


class ListTaskRelationsResponse(_BaseSchema):
    """Response with task relations."""

    relations: list[TaskRelation]


class ListWorkersRequest(_BaseSchema):
    """Request to list workers."""


class ListWorkersResponse(_BaseSchema):
    """Response with workers."""

    workers: list[Worker]


class GetWorkerRequest(_BaseSchema):
    """Request to fetch a worker by hostname."""

    hostname: str


class GetWorkerResponse(_BaseSchema):
    """Response with a worker record."""

    worker: Worker | None


class TaskStatsRequest(_BaseSchema):
    """Request to compute task statistics."""

    task_name: str | None = None
    time_range: TimeRange | None = None


class TaskStatsResponse(_BaseSchema):
    """Response with task statistics."""

    stats: TaskStats


class ThroughputRequest(_BaseSchema):
    """Request throughput data."""

    time_range: TimeRange
    bucket_seconds: int


class ThroughputResponse(_BaseSchema):
    """Response with throughput buckets."""

    buckets: list[ThroughputBucket]


class StateDistributionRequest(_BaseSchema):
    """Request task state distribution counts."""


class StateDistributionResponse(_BaseSchema):
    """Response with state distribution."""

    counts: dict[str, int]


class HeatmapRequest(_BaseSchema):
    """Request heatmap data."""

    time_range: TimeRange | None = None


class HeatmapResponse(_BaseSchema):
    """Response with heatmap data."""

    heatmap: list[list[int]]


class ListSchedulesRequest(_BaseSchema):
    """Request list of schedules."""


class ListSchedulesResponse(_BaseSchema):
    """Response with schedules."""

    schedules: list[Schedule]


class StoreScheduleRequest(_BaseSchema):
    """Request to store a schedule."""

    schedule: Schedule


class DeleteScheduleRequest(_BaseSchema):
    """Request to delete a schedule."""

    schedule_id: str


class CleanupRequest(_BaseSchema):
    """Request to cleanup old records."""

    older_than_days: int


class CleanupResponse(_BaseSchema):
    """Response with cleanup results."""

    removed: int


class SchemaVersionRequest(_BaseSchema):
    """Request current schema version."""


class SchemaVersionResponse(_BaseSchema):
    """Response with schema version."""

    version: int


class SchemaRequest(_BaseSchema):
    """Request database schema metadata."""


class SchemaColumn(_BaseSchema):
    """Schema column metadata."""

    name: str
    type: str
    nullable: bool
    default: Any | None = None
    primary_key: bool = False


class SchemaIndex(_BaseSchema):
    """Schema index metadata."""

    name: str | None = None
    column_names: list[str]
    unique: bool | None = None


class SchemaTable(_BaseSchema):
    """Schema table metadata."""

    columns: list[SchemaColumn]
    indexes: list[SchemaIndex]


class SchemaResponse(_BaseSchema):
    """Response with schema metadata."""

    dialect: str
    tables: dict[str, SchemaTable]
