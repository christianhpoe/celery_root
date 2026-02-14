# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Abstract database controller interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence

    from celery_root.core.db.models import (
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


class BaseDBController(ABC):
    """Subclass to provide a custom storage backend."""

    @abstractmethod
    def initialize(self) -> None:
        """Initialize backend schema and storage."""
        ...

    @abstractmethod
    def get_schema_version(self) -> int:
        """Return the current schema version."""
        ...

    @abstractmethod
    def ensure_schema(self) -> None:
        """Ensure the backend schema is up to date."""
        ...

    @abstractmethod
    def migrate(self, from_version: int, to_version: int) -> None:
        """Migrate the storage schema to a target version."""
        ...

    @abstractmethod
    def store_task_event(self, event: TaskEvent) -> None:
        """Persist a task event."""
        ...

    @abstractmethod
    def get_tasks(self, filters: TaskFilter | None = None) -> Sequence[Task]:
        """Return tasks matching optional filters."""
        ...

    @abstractmethod
    def get_tasks_page(
        self,
        filters: TaskFilter | None,
        *,
        sort_key: str | None,
        sort_dir: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[Task], int]:
        """Return paginated tasks and total count."""
        ...

    @abstractmethod
    def list_task_names(self) -> Sequence[str]:
        """Return distinct task names stored in the DB."""
        ...

    @abstractmethod
    def get_task(self, task_id: str) -> Task | None:
        """Return a task by ID, if present."""
        ...

    @abstractmethod
    def store_task_relation(self, relation: TaskRelation) -> None:
        """Persist a task relation edge."""
        ...

    @abstractmethod
    def get_task_relations(self, root_id: str) -> Sequence[TaskRelation]:
        """Return task relations for a root task."""
        ...

    @abstractmethod
    def store_worker_event(self, event: WorkerEvent) -> None:
        """Persist a worker event."""
        ...

    @abstractmethod
    def get_workers(self) -> Sequence[Worker]:
        """Return all known workers."""
        ...

    @abstractmethod
    def get_worker(self, hostname: str) -> Worker | None:
        """Return a worker by hostname, if present."""
        ...

    @abstractmethod
    def get_task_stats(self, task_name: str | None, time_range: TimeRange | None) -> TaskStats:
        """Return aggregated task statistics."""
        ...

    @abstractmethod
    def get_throughput(self, time_range: TimeRange, bucket_seconds: int) -> Sequence[ThroughputBucket]:
        """Return throughput buckets for the time range."""
        ...

    @abstractmethod
    def get_state_distribution(self) -> dict[str, int]:
        """Return counts by task state."""
        ...

    @abstractmethod
    def get_heatmap(self, time_range: TimeRange | None) -> list[list[int]]:
        """Return a heatmap of task activity."""
        ...

    @abstractmethod
    def get_schedules(self) -> Sequence[Schedule]:
        """Return all stored schedules."""
        ...

    @abstractmethod
    def store_schedule(self, schedule: Schedule) -> None:
        """Persist a schedule entry."""
        ...

    @abstractmethod
    def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule entry by ID."""
        ...

    @abstractmethod
    def cleanup(self, older_than_days: int) -> int:
        """Delete historical data older than the retention window."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close any backend resources."""
        ...
