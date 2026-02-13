# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Abstract monitoring exporter interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery_root.core.db.models import TaskEvent, TaskStats, WorkerEvent


class BaseMonitoringExporter(ABC):
    """Base class for monitoring exporters."""

    @abstractmethod
    def on_task_event(self, event: TaskEvent) -> None:
        """Handle a task event."""
        ...

    @abstractmethod
    def on_worker_event(self, event: WorkerEvent) -> None:
        """Handle a worker event."""
        ...

    @abstractmethod
    def update_stats(self, stats: TaskStats) -> None:
        """Handle periodic task statistics."""
        ...

    @abstractmethod
    def serve(self) -> None:
        """Start serving exporter data."""
        ...

    @abstractmethod
    def shutdown(self) -> None:
        """Shut down exporter resources."""
        ...
