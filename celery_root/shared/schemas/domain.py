# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Shared domain schemas used across Celery Root components."""

from __future__ import annotations

import datetime as _dt
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

Datetime = _dt.datetime


class _BaseSchema(BaseModel):
    """Base schema with shared configuration."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True, validate_assignment=False)


class TimeRange(_BaseSchema):
    """Time window for queries."""

    start: Datetime
    end: Datetime


class TaskEvent(_BaseSchema):
    """Incoming task event snapshot."""

    task_id: str
    name: str | None
    state: str
    timestamp: Datetime
    worker: str | None = None
    args: str | None = None
    kwargs_: str | None = Field(default=None, alias="kwargs")
    result: str | None = None
    traceback: str | None = None
    stamps: str | None = None
    runtime: float | None = None
    retries: int | None = None
    eta: Datetime | None = None
    expires: Datetime | None = None
    parent_id: str | None = None
    root_id: str | None = None
    group_id: str | None = None
    chord_id: str | None = None


class WorkerEvent(_BaseSchema):
    """Incoming worker event snapshot."""

    hostname: str
    event: str
    timestamp: Datetime
    info: dict[str, Any] | None = None
    broker_url: str | None = None


class BrokerQueueEvent(_BaseSchema):
    """Incoming broker queue snapshot."""

    broker_url: str
    queue: str
    messages: int | None = None
    consumers: int | None = None
    timestamp: Datetime


class Task(_BaseSchema):
    """Stored task record."""

    task_id: str
    name: str | None
    state: str
    worker: str | None = None
    received: Datetime | None = None
    started: Datetime | None = None
    finished: Datetime | None = None
    runtime: float | None = None
    args: str | None = None
    kwargs_: str | None = Field(default=None, alias="kwargs")
    result: str | None = None
    traceback: str | None = None
    stamps: str | None = None
    retries: int | None = None
    parent_id: str | None = None
    root_id: str | None = None
    group_id: str | None = None
    chord_id: str | None = None


class Worker(_BaseSchema):
    """Stored worker record."""

    hostname: str
    status: str
    last_heartbeat: Datetime | None = None
    pool_size: int | None = None
    active_tasks: int | None = None
    registered_tasks: list[str] | None = None
    queues: list[str] | None = None
    broker_url: str | None = None


class Schedule(_BaseSchema):
    """Stored beat schedule record."""

    schedule_id: str
    name: str
    task: str
    schedule: str
    args: str | None = None
    kwargs_: str | None = Field(default=None, alias="kwargs")
    enabled: bool = True
    last_run_at: Datetime | None = None
    total_run_count: int | None = None
    app: str | None = None


class TaskRelation(_BaseSchema):
    """Relation between tasks in a workflow graph."""

    root_id: str
    parent_id: str | None
    child_id: str
    relation: str


class TaskFilter(_BaseSchema):
    """Filter options for task queries."""

    task_name: str | None = None
    state: str | None = None
    worker: str | None = None
    time_range: TimeRange | None = None
    search: str | None = None
    group_id: str | None = None
    root_id: str | None = None


class TaskStats(_BaseSchema):
    """Aggregated task statistics."""

    count: int = 0
    min_runtime: float | None = None
    max_runtime: float | None = None
    avg_runtime: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


class WorkerStats(_BaseSchema):
    """Aggregated worker statistics."""

    hostname: str
    status: str
    active: int | None = None
    processed: int | None = None
    loadavg: tuple[float, float, float] | None = None


class ThroughputBucket(_BaseSchema):
    """Counts for a throughput time bucket."""

    bucket_start: Datetime
    count: int
