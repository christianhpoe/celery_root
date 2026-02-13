# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Convenience helpers for Root operations."""

from .backend import StoredResult, clear_results, list_results
from .beat import delete_schedule, detect_backend, list_schedules, save_schedule
from .brokers import QueueInfo, list_queues, purge_queues
from .health import health_check
from .retry import smart_retry
from .tasks import rate_limit, revoke, send_task, time_limit
from .workers import add_consumer, autoscale, get_stats, pool_grow, pool_shrink, remove_consumer, restart, shutdown

__all__ = [
    "QueueInfo",
    "StoredResult",
    "add_consumer",
    "autoscale",
    "clear_results",
    "delete_schedule",
    "detect_backend",
    "get_stats",
    "health_check",
    "list_queues",
    "list_results",
    "list_schedules",
    "pool_grow",
    "pool_shrink",
    "purge_queues",
    "rate_limit",
    "remove_consumer",
    "restart",
    "revoke",
    "save_schedule",
    "send_task",
    "shutdown",
    "smart_retry",
    "time_limit",
]
