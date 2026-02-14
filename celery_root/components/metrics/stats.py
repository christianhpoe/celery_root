# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Convenience wrappers for DB statistics access."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery_root.core.db import DbClient
    from celery_root.core.db.models import TaskStats, ThroughputBucket, TimeRange


def task_runtime_stats(db: DbClient, task_name: str | None, time_range: TimeRange | None) -> TaskStats:
    """Fetch runtime statistics for a task."""
    return db.get_task_stats(task_name, time_range)


def throughput(db: DbClient, time_range: TimeRange, bucket_seconds: int) -> list[ThroughputBucket]:
    """Fetch throughput buckets for the time range."""
    return list(db.get_throughput(time_range, bucket_seconds))


def state_distribution(db: DbClient) -> dict[str, int]:
    """Fetch task counts by state."""
    return db.get_state_distribution()


def heatmap_data(db: DbClient, time_range: TimeRange | None) -> list[list[int]]:
    """Fetch heatmap data for task activity."""
    return db.get_heatmap(time_range)
