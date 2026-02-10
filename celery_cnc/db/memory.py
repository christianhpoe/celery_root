"""In-memory database controller implementation."""

from __future__ import annotations

import functools
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from .abc import BaseDBController
from .models import (
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

if TYPE_CHECKING:
    from collections.abc import Iterable

_FINAL_STATES = {"SUCCESS", "FAILURE", "REVOKED"}


def _coerce_dt(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _task_timestamp(task: Task) -> datetime | None:
    return task.finished or task.started or task.received


def _matches_search(task: Task, search: str) -> bool:
    haystack = (f"{task.name or ''} {task.task_id} {task.worker or ''} {task.args or ''} {task.kwargs or ''}").lower()
    return search.lower() in haystack


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if pct <= 0:
        return values[0]
    if pct >= 1:
        return values[-1]
    index = (len(values) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    weight = index - lower
    return values[lower] + (values[upper] - values[lower]) * weight


class MemoryController(BaseDBController):
    """In-memory controller."""

    def __init__(self) -> None:
        """Initialize empty in-memory storage."""
        self._initialized = False
        self._schema_version = 4
        self._tasks: dict[str, Task] = {}
        self._task_events: list[TaskEvent] = []
        self._task_relations: list[TaskRelation] = []
        self._workers: dict[str, Worker] = {}
        self._worker_events: list[WorkerEvent] = []
        self._schedules: dict[str, Schedule] = {}

    def initialize(self) -> None:
        """Mark the controller as initialized."""
        self._initialized = True

    def get_schema_version(self) -> int:
        """Return the in-memory schema version."""
        return self._schema_version if self._initialized else 0

    def migrate(self, from_version: int, to_version: int) -> None:
        """Update the stored schema version when needed."""
        if from_version != to_version:
            self._schema_version = to_version

    def store_task_event(self, event: TaskEvent) -> None:
        """Store a task event and update task state."""
        self._task_events.append(event)
        task = self._tasks.get(event.task_id)
        if task is None:
            task = Task(task_id=event.task_id, name=event.name, state=event.state)
            self._tasks[event.task_id] = task
        self._apply_task_event(task, event)

    def get_tasks(self, filters: TaskFilter | None = None) -> list[Task]:
        """Return tasks optionally filtered by criteria."""
        tasks = list(self._tasks.values())
        if filters is None:
            return self._sorted_tasks(tasks)
        tasks = [task for task in tasks if self._task_matches(task, filters)]
        return self._sorted_tasks(tasks)

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
        tasks = list(self._tasks.values())
        if filters is not None:
            tasks = [task for task in tasks if self._task_matches(task, filters)]
        total = len(tasks)
        tasks = self._sort_tasks_by(tasks, sort_key, sort_dir) if sort_key else self._sorted_tasks(tasks)
        return tasks[offset : offset + limit], total

    def list_task_names(self) -> list[str]:
        """Return distinct task names stored in memory."""
        names = {task.name or "unknown" for task in self._tasks.values()}
        return sorted(names)

    def get_task(self, task_id: str) -> Task | None:
        """Return a task by ID if present."""
        return self._tasks.get(task_id)

    def store_task_relation(self, relation: TaskRelation) -> None:
        """Store a task relation edge."""
        self._task_relations.append(relation)

    def get_task_relations(self, root_id: str) -> list[TaskRelation]:
        """Return relations for a root task."""
        return [rel for rel in self._task_relations if rel.root_id == root_id]

    def store_worker_event(self, event: WorkerEvent) -> None:
        """Store a worker event and update worker info."""
        self._worker_events.append(event)
        worker = self._workers.get(event.hostname)
        status = self._status_from_worker_event(event.event)
        if worker is None:
            worker = Worker(hostname=event.hostname, status=status)
            self._workers[event.hostname] = worker
        worker.status = status
        if event.event in {"worker-heartbeat", "worker-online"}:
            worker.last_heartbeat = event.timestamp
        info = event.info or {}
        if isinstance(info, dict):
            active = info.get("active")
            if isinstance(active, list):
                worker.active_tasks = len(active)
            pool = info.get("pool")
            if isinstance(pool, dict):
                pool_size = pool.get("max-concurrency") or pool.get("max_concurrency")
                if isinstance(pool_size, int):
                    worker.pool_size = pool_size
            registered = info.get("registered")
            if isinstance(registered, list):
                worker.registered_tasks = [str(item) for item in registered]
            queues = info.get("queues")
            if isinstance(queues, list):
                names: list[str] = []
                for entry in queues:
                    if isinstance(entry, dict) and "name" in entry:
                        names.append(str(entry["name"]))
                    elif isinstance(entry, str):
                        names.append(entry)
                worker.queues = names or None
        if event.broker_url:
            worker.broker_url = event.broker_url

    def get_workers(self) -> list[Worker]:
        """Return all workers."""
        return list(self._workers.values())

    def get_worker(self, hostname: str) -> Worker | None:
        """Return a worker by hostname."""
        return self._workers.get(hostname)

    def get_task_stats(self, task_name: str | None, time_range: TimeRange | None) -> TaskStats:
        """Compute task runtime statistics."""
        tasks = self._filter_tasks(task_name, time_range)
        runtimes = sorted(
            [task.runtime for task in tasks if task.runtime is not None],
        )
        count = len(tasks)
        if not runtimes:
            return TaskStats(count=count)
        return TaskStats(
            count=count,
            min_runtime=runtimes[0],
            max_runtime=runtimes[-1],
            avg_runtime=sum(runtimes) / len(runtimes),
            p50=_percentile(runtimes, 0.5),
            p95=_percentile(runtimes, 0.95),
            p99=_percentile(runtimes, 0.99),
        )

    def get_throughput(self, time_range: TimeRange, bucket_seconds: int) -> list[ThroughputBucket]:
        """Compute throughput buckets for tasks."""
        buckets = self._init_buckets(time_range, bucket_seconds)
        for task in self._filter_tasks(None, time_range):
            timestamp = _task_timestamp(task)
            if timestamp is None:
                continue
            bucket_start = self._bucket_start(time_range.start, timestamp, bucket_seconds)
            if bucket_start in buckets:
                buckets[bucket_start] += 1
        return [ThroughputBucket(bucket_start=key, count=value) for key, value in buckets.items()]

    def get_state_distribution(self) -> dict[str, int]:
        """Return task counts by state."""
        distribution: dict[str, int] = defaultdict(int)
        for task in self._tasks.values():
            distribution[task.state] += 1
        return dict(distribution)

    def get_heatmap(self, time_range: TimeRange | None) -> list[list[int]]:
        """Return heatmap data for task activity."""
        heatmap = [[0 for _ in range(24)] for _ in range(7)]
        tasks = self._filter_tasks(None, time_range)
        for task in tasks:
            timestamp = _task_timestamp(task)
            if timestamp is None:
                continue
            timestamp = _coerce_dt(timestamp)
            heatmap[timestamp.weekday()][timestamp.hour] += 1
        return heatmap

    def get_schedules(self) -> list[Schedule]:
        """Return all schedules."""
        return list(self._schedules.values())

    def store_schedule(self, schedule: Schedule) -> None:
        """Persist a schedule entry."""
        self._schedules[schedule.schedule_id] = schedule

    def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule entry."""
        self._schedules.pop(schedule_id, None)

    def cleanup(self, older_than_days: int) -> int:
        """Remove historical data older than the cutoff."""
        cutoff = _coerce_dt(datetime.now(UTC) - timedelta(days=older_than_days))
        removed = 0
        before_events = len(self._task_events)
        self._task_events = [event for event in self._task_events if _coerce_dt(event.timestamp) >= cutoff]
        removed += before_events - len(self._task_events)
        before_workers = len(self._worker_events)
        self._worker_events = [event for event in self._worker_events if _coerce_dt(event.timestamp) >= cutoff]
        removed += before_workers - len(self._worker_events)
        tasks_before = len(self._tasks)
        self._tasks = {
            task_id: task
            for task_id, task in self._tasks.items()
            if (ts := _task_timestamp(task)) is None or _coerce_dt(ts) >= cutoff
        }
        removed += tasks_before - len(self._tasks)
        workers_before = len(self._workers)
        self._workers = {
            hostname: worker
            for hostname, worker in self._workers.items()
            if worker.last_heartbeat is None or _coerce_dt(worker.last_heartbeat) >= cutoff
        }
        removed += workers_before - len(self._workers)
        return removed

    def close(self) -> None:
        """Close resources (noop for memory)."""
        return

    @staticmethod
    def _sorted_tasks(tasks: Iterable[Task]) -> list[Task]:
        return sorted(
            tasks,
            key=MemoryController._task_sort_key,
            reverse=True,
        )

    @staticmethod
    def _sort_tasks_by(tasks: Iterable[Task], sort_key: str, sort_dir: str | None) -> list[Task]:
        reverse = (sort_dir or "desc").lower() == "desc"

        def _value(task: Task) -> object:
            if sort_key == "state":
                value: object = task.state
            elif sort_key == "worker":
                value = task.worker
            elif sort_key == "received":
                value = task.received
            elif sort_key == "started":
                value = task.started
            elif sort_key == "runtime":
                value = task.runtime
            else:
                value = _task_timestamp(task)
            if isinstance(value, str):
                return value.lower()
            if isinstance(value, datetime):
                return _coerce_dt(value)
            return value

        def _compare(left: Task, right: Task) -> int:
            left_value = _value(left)
            right_value = _value(right)
            if left_value is None and right_value is None:
                return 0
            if left_value is None:
                return 1
            if right_value is None:
                return -1
            if left_value == right_value:
                return 0
            comparison = 0
            match (left_value, right_value):
                case (str() as left_str, str() as right_str):
                    comparison = (left_str > right_str) - (left_str < right_str)
                case (datetime() as left_dt, datetime() as right_dt):
                    comparison = (left_dt > right_dt) - (left_dt < right_dt)
                case ((int() | float()) as left_num, (int() | float()) as right_num):
                    comparison = (left_num > right_num) - (left_num < right_num)
                case _:
                    comparison = 0
            return -comparison if reverse else comparison

        return sorted(tasks, key=functools.cmp_to_key(_compare))

    @staticmethod
    def _task_sort_key(task: Task) -> datetime:
        timestamp = _task_timestamp(task)
        if timestamp is None:
            return datetime.min.replace(tzinfo=UTC)
        return _coerce_dt(timestamp)

    @staticmethod
    def _status_from_worker_event(event_name: str) -> str:
        if event_name == "worker-offline":
            return "OFFLINE"
        return "ONLINE"

    @staticmethod
    def _apply_task_event(task: Task, event: TaskEvent) -> None:
        preserve = MemoryController._should_preserve_state(task.state, event.state)
        if not preserve:
            task.state = event.state
        if not preserve:
            updates = {
                "name": event.name,
                "worker": event.worker,
                "args": event.args,
                "kwargs": event.kwargs,
                "result": event.result,
                "traceback": event.traceback,
                "stamps": event.stamps,
                "runtime": event.runtime,
                "retries": event.retries,
                "parent_id": event.parent_id,
                "root_id": event.root_id,
                "group_id": event.group_id,
                "chord_id": event.chord_id,
            }
            for field_name, value in updates.items():
                if value is not None:
                    setattr(task, field_name, value)
            if event.state == "RECEIVED" or (event.state == "PENDING" and task.received is None):
                task.received = event.timestamp
            elif event.state == "STARTED":
                task.started = event.timestamp
            elif event.state in _FINAL_STATES:
                task.finished = event.timestamp

    @staticmethod
    def _should_preserve_state(existing_state: str, incoming_state: str) -> bool:
        if existing_state in _FINAL_STATES and incoming_state not in _FINAL_STATES:
            return True
        if existing_state == "STARTED" and incoming_state in {"PENDING", "RECEIVED"}:
            return True
        return existing_state == "RECEIVED" and incoming_state == "PENDING"

    def _task_matches(self, task: Task, filters: TaskFilter) -> bool:
        matches = True
        if (
            (filters.task_name and task.name != filters.task_name)
            or (filters.state and task.state != filters.state)
            or (filters.worker and task.worker != filters.worker)
            or (filters.group_id and task.group_id != filters.group_id)
            or (filters.root_id and task.root_id != filters.root_id)
        ):
            matches = False
        if matches and filters.search and not _matches_search(task, filters.search):
            matches = False
        if matches and filters.time_range:
            timestamp = _task_timestamp(task)
            if timestamp is None:
                matches = False
            else:
                timestamp = _coerce_dt(timestamp)
                start = _coerce_dt(filters.time_range.start)
                end = _coerce_dt(filters.time_range.end)
                matches = start <= timestamp <= end
        return matches

    def _filter_tasks(self, task_name: str | None, time_range: TimeRange | None) -> list[Task]:
        tasks = list(self._tasks.values())
        if task_name is not None:
            tasks = [task for task in tasks if task.name == task_name]
        if time_range is not None:
            start = _coerce_dt(time_range.start)
            end = _coerce_dt(time_range.end)
            tasks = [
                task for task in tasks if (ts := _task_timestamp(task)) is not None and start <= _coerce_dt(ts) <= end
            ]
        return tasks

    @staticmethod
    def _init_buckets(time_range: TimeRange, bucket_seconds: int) -> dict[datetime, int]:
        start = _coerce_dt(time_range.start)
        end = _coerce_dt(time_range.end)
        buckets: dict[datetime, int] = {}
        cursor = start
        while cursor <= end:
            buckets[cursor] = 0
            cursor = cursor + timedelta(seconds=bucket_seconds)
        return buckets

    @staticmethod
    def _bucket_start(start: datetime, timestamp: datetime, bucket_seconds: int) -> datetime:
        start = _coerce_dt(start)
        timestamp = _coerce_dt(timestamp)
        delta_seconds = int((timestamp - start).total_seconds())
        bucket_index = max(delta_seconds // bucket_seconds, 0)
        return start + timedelta(seconds=bucket_index * bucket_seconds)
