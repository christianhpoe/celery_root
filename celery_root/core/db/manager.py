# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""DB manager process for ingesting events."""

from __future__ import annotations

import logging
import time
from contextlib import suppress
from multiprocessing import Event, Process, Queue
from queue import Empty
from typing import TYPE_CHECKING

from celery_root.config import set_settings
from celery_root.core.logging.setup import configure_process_logging

from .models import TaskEvent, TaskRelation, WorkerEvent

if TYPE_CHECKING:
    from collections.abc import Callable

    from celery_root.config import CeleryRootConfig

    from .adapters.base import BaseDBController

_SCHEMA_VERSION = 4
_HEARTBEAT_INTERVAL = 60.0


class DBManager(Process):
    """Reads events from the queue and writes to the DB."""

    def __init__(
        self,
        queue: Queue[object],
        controller_factory: Callable[[], BaseDBController],
        config: CeleryRootConfig,
    ) -> None:
        """Create a DB manager process."""
        super().__init__(daemon=True)
        self._queue = queue
        self._controller_factory = controller_factory
        self._config = config
        self._stop_event = Event()
        self._logger = logging.getLogger(__name__)

    def stop(self) -> None:
        """Signal the DB manager to stop."""
        self._stop_event.set()
        with suppress(Exception):  # pragma: no cover - best effort
            self._queue.put_nowait(None)

    def run(self) -> None:
        """Run the ingestion loop and write events to storage."""
        set_settings(self._config)
        configure_process_logging(self._config, component="db_manager")
        self._logger.info("DBManager starting.")
        controller = self._controller_factory()
        controller.initialize()
        current_version = controller.get_schema_version()
        if current_version != _SCHEMA_VERSION:
            self._logger.info(
                "DBManager migrating schema from %d to %d.",
                current_version,
                _SCHEMA_VERSION,
            )
            controller.migrate(current_version, _SCHEMA_VERSION)

        batch: list[object] = []
        last_flush = time.monotonic()
        last_heartbeat = last_flush

        try:
            while True:
                if self._stop_event.is_set() and self._queue.empty():
                    break
                timeout = max(self._config.database.flush_interval - (time.monotonic() - last_flush), 0.0)
                try:
                    item = self._queue.get(timeout=timeout)
                except Empty:
                    item = None

                now = time.monotonic()
                if now - last_heartbeat >= _HEARTBEAT_INTERVAL:
                    self._logger.info("DBManager heartbeat.")
                    last_heartbeat = now

                if item is None:
                    if self._stop_event.is_set():
                        break
                else:
                    batch.append(item)

                if batch and (
                    len(batch) >= self._config.database.batch_size
                    or (now - last_flush) >= self._config.database.flush_interval
                ):
                    self._flush(batch, controller)
                    last_flush = now
        except KeyboardInterrupt:
            self._logger.info("DBManager interrupted; shutting down.")
            self._stop_event.set()
        finally:
            if batch:
                self._flush(batch, controller)
            controller.close()
            self._logger.info("DBManager stopped.")

    def _flush(self, batch: list[object], controller: BaseDBController) -> None:
        task_events = 0
        worker_events = 0
        other_events = 0
        for item in batch:
            if isinstance(item, TaskEvent):
                controller.store_task_event(item)
                self._store_relations(controller, item)
                task_events += 1
            elif isinstance(item, WorkerEvent):
                controller.store_worker_event(item)
                worker_events += 1
            elif isinstance(item, TaskRelation):
                controller.store_task_relation(item)
                other_events += 1
            else:  # pragma: no cover - defensive
                self._logger.debug("DBManager ignored unknown event %r", item)
                other_events += 1
        if batch:
            self._logger.debug(
                "DBManager flushed %d task events, %d worker events, %d other items.",
                task_events,
                worker_events,
                other_events,
            )
        batch.clear()

    @staticmethod
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
