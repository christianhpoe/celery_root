# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Dataclass models used by database controllers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import datetime


@dataclass(slots=True)
class TimeRange:
    """Time window for queries."""

    start: datetime
    end: datetime


@dataclass(slots=True)
class TaskEvent:
    """Incoming task event snapshot."""

    task_id: str
    name: str | None
    state: str
    timestamp: datetime
    worker: str | None = None
    args: str | None = None
    kwargs: str | None = None
    result: str | None = None
    traceback: str | None = None
    stamps: str | None = None
    runtime: float | None = None
    retries: int | None = None
    eta: datetime | None = None
    expires: datetime | None = None
    parent_id: str | None = None
    root_id: str | None = None
    group_id: str | None = None
    chord_id: str | None = None


@dataclass(slots=True)
class WorkerEvent:
    """Incoming worker event snapshot."""

    hostname: str
    event: str
    timestamp: datetime
    info: dict[str, object] | None = None
    broker_url: str | None = None


@dataclass(slots=True)
class Task:
    """Stored task record."""

    task_id: str
    name: str | None
    state: str
    worker: str | None = None
    received: datetime | None = None
    started: datetime | None = None
    finished: datetime | None = None
    runtime: float | None = None
    args: str | None = None
    kwargs: str | None = None
    result: str | None = None
    traceback: str | None = None
    stamps: str | None = None
    retries: int | None = None
    parent_id: str | None = None
    root_id: str | None = None
    group_id: str | None = None
    chord_id: str | None = None


@dataclass(slots=True)
class Worker:
    """Stored worker record."""

    hostname: str
    status: str
    last_heartbeat: datetime | None = None
    pool_size: int | None = None
    active_tasks: int | None = None
    registered_tasks: list[str] | None = None
    queues: list[str] | None = None
    broker_url: str | None = None


@dataclass(slots=True)
class Schedule:
    """Stored beat schedule record."""

    schedule_id: str
    name: str
    task: str
    schedule: str
    args: str | None = None
    kwargs: str | None = None
    enabled: bool = True
    last_run_at: datetime | None = None
    total_run_count: int | None = None
    app: str | None = None


@dataclass(slots=True)
class TaskRelation:
    """Relation between tasks in a workflow graph."""

    root_id: str
    parent_id: str | None
    child_id: str
    relation: str


@dataclass(slots=True)
class TaskFilter:
    """Filter options for task queries."""

    task_name: str | None = None
    state: str | None = None
    worker: str | None = None
    time_range: TimeRange | None = None
    search: str | None = None
    group_id: str | None = None
    root_id: str | None = None


@dataclass(slots=True)
class TaskStats:
    """Aggregated task statistics."""

    count: int = 0
    min_runtime: float | None = None
    max_runtime: float | None = None
    avg_runtime: float | None = None
    p50: float | None = None
    p95: float | None = None
    p99: float | None = None


@dataclass(slots=True)
class WorkerStats:
    """Aggregated worker statistics."""

    hostname: str
    status: str
    active: int | None = None
    processed: int | None = None
    loadavg: tuple[float, float, float] | None = None


@dataclass(slots=True)
class ThroughputBucket:
    """Counts for a throughput time bucket."""

    bucket_start: datetime
    count: int
