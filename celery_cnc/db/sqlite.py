"""SQLite-backed database controller implementation."""

from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    delete,
    event,
    func,
    select,
    text,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

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
    from collections.abc import Mapping
    from sqlite3 import Connection as SQLiteConnection

    from sqlalchemy.engine import Connection, Engine
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.elements import ColumnElement


_FINAL_STATES = {"SUCCESS", "FAILURE", "REVOKED"}
_SQLITE_BUSY_TIMEOUT_MS = 30_000
_SCHEDULE_APP_SCHEMA_VERSION = 2
_BROKER_URL_SCHEMA_VERSION = 3
_STAMPS_SCHEMA_VERSION = 4


def _configure_sqlite(dbapi_connection: SQLiteConnection, _connection_record: object) -> None:
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute(f"PRAGMA busy_timeout={_SQLITE_BUSY_TIMEOUT_MS}")
    cursor.close()


def _coerce_dt(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _task_timestamp(task: Task) -> datetime | None:
    return task.finished or task.started or task.received


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


def _row_dict(row: object) -> dict[str, object]:
    mapping = getattr(row, "_mapping", None)
    if mapping is None:
        return dict(cast("Mapping[str, object]", row))
    return dict(cast("Mapping[str, object]", mapping))


def _as_str(value: object) -> str:
    return cast("str", value)


def _as_optional_str(value: object) -> str | None:
    return cast("str | None", value)


def _as_optional_int(value: object) -> int | None:
    return cast("int | None", value)


def _as_optional_float(value: object) -> float | None:
    return cast("float | None", value)


def _as_optional_datetime(value: object) -> datetime | None:
    return cast("datetime | None", value)


class SQLiteController(BaseDBController):
    """SQLite-backed controller."""

    _SCHEMA_VERSION = 4

    def __init__(self, path: str | Path = "celery_cnc.db") -> None:
        """Initialize the SQLite controller with a database path."""
        self._path = Path(path).expanduser().resolve()
        self._ensure_writable_path()
        self._engine: Engine = create_engine(
            f"sqlite:///{self._path}",
            future=True,
            connect_args={"timeout": _SQLITE_BUSY_TIMEOUT_MS / 1000},
        )
        event.listen(self._engine, "connect", _configure_sqlite)
        self._metadata = MetaData()
        self._define_tables()

    def initialize(self) -> None:
        """Create tables and initialize schema version."""
        self._metadata.create_all(self._engine)
        with self._engine.begin() as conn:
            existing = conn.execute(select(self._schema_version.c.version)).scalar_one_or_none()
            if existing is None:
                conn.execute(self._schema_version.insert().values(version=self._SCHEMA_VERSION))

    def get_schema_version(self) -> int:
        """Return the stored schema version."""
        with self._engine.begin() as conn:
            version = conn.execute(select(self._schema_version.c.version)).scalar_one_or_none()
        return int(version or 0)

    def migrate(self, from_version: int, to_version: int) -> None:
        """Migrate schema version metadata."""
        if from_version == to_version:
            return
        with self._engine.begin() as conn:
            if from_version < _SCHEDULE_APP_SCHEMA_VERSION <= to_version:
                conn.execute(text("ALTER TABLE schedules ADD COLUMN app TEXT"))
            if from_version < _BROKER_URL_SCHEMA_VERSION <= to_version:
                conn.execute(text("ALTER TABLE worker_events ADD COLUMN broker_url TEXT"))
                conn.execute(text("ALTER TABLE workers ADD COLUMN broker_url TEXT"))
            if from_version < _STAMPS_SCHEMA_VERSION <= to_version:
                conn.execute(text("ALTER TABLE tasks ADD COLUMN stamps TEXT"))
                conn.execute(text("ALTER TABLE task_events ADD COLUMN stamps TEXT"))
            conn.execute(self._schema_version.delete())
            conn.execute(self._schema_version.insert().values(version=to_version))

    def store_task_event(self, event: TaskEvent) -> None:
        """Persist a task event and update the task record."""
        event_values = self._event_values(event)
        with self._engine.begin() as conn:
            existing_state = self._get_task_state(conn, event.task_id)
            task_values = self._task_values_from_event(event, existing_state)
            update_values = dict(task_values)
            update_values.pop("task_id", None)
            stmt = sqlite_insert(self._tasks).values(**task_values)
            stmt = stmt.on_conflict_do_update(
                index_elements=[self._tasks.c.task_id],
                set_=update_values,
            )
            conn.execute(self._task_events.insert().values(**event_values))
            conn.execute(stmt)

    def get_tasks(self, filters: TaskFilter | None = None) -> list[Task]:
        """Return tasks matching optional filters."""
        stmt: Select[tuple[object, ...]] = select(self._tasks)
        ts_col = func.coalesce(
            self._tasks.c.finished,
            self._tasks.c.started,
            self._tasks.c.received,
        )
        if filters:
            stmt = self._apply_task_filters(stmt, filters, ts_col)
        stmt = stmt.order_by(ts_col.desc())
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return [self._row_to_task(_row_dict(row)) for row in rows]

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
        stmt: Select[tuple[object, ...]] = select(self._tasks)
        count_stmt = cast("Select[tuple[object, ...]]", select(func.count()).select_from(self._tasks))
        ts_col = func.coalesce(
            self._tasks.c.finished,
            self._tasks.c.started,
            self._tasks.c.received,
        )
        if filters:
            stmt = self._apply_task_filters(stmt, filters, ts_col)
            count_stmt = self._apply_task_filters(count_stmt, filters, ts_col)

        sort_column = self._task_sort_column(sort_key, ts_col)
        direction = (sort_dir or "desc").lower()
        stmt = stmt.order_by(sort_column.asc()) if direction == "asc" else stmt.order_by(sort_column.desc())
        stmt = stmt.limit(limit).offset(offset)

        with self._engine.begin() as conn:
            total_raw = conn.execute(count_stmt).scalar_one()
            rows = conn.execute(stmt).all()
        total = int(total_raw) if isinstance(total_raw, (int, float)) else int(total_raw or 0)
        return [self._row_to_task(_row_dict(row)) for row in rows], total

    def list_task_names(self) -> list[str]:
        """Return distinct task names stored in the DB."""
        stmt = select(self._tasks.c.name).distinct()
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        names: list[str] = []
        unknown = False
        for row in rows:
            name = row[0]
            if name is None:
                unknown = True
            else:
                names.append(str(name))
        if unknown:
            names.append("unknown")
        return sorted(names)

    def get_task(self, task_id: str) -> Task | None:
        """Return a task by ID, if present."""
        stmt = select(self._tasks).where(self._tasks.c.task_id == task_id)
        with self._engine.begin() as conn:
            row = conn.execute(stmt).first()
        return self._row_to_task(_row_dict(row)) if row else None

    def store_task_relation(self, relation: TaskRelation) -> None:
        """Persist a task relation edge."""
        with self._engine.begin() as conn:
            conn.execute(self._task_relations.insert().values(**asdict(relation)))

    def get_task_relations(self, root_id: str) -> list[TaskRelation]:
        """Return task relations for a root task."""
        stmt = select(self._task_relations).where(self._task_relations.c.root_id == root_id)
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return [self._row_to_relation(_row_dict(row)) for row in rows]

    def store_worker_event(self, event: WorkerEvent) -> None:
        """Persist a worker event and update worker state."""
        info_json = json.dumps(event.info) if event.info is not None else None
        with self._engine.begin() as conn:
            conn.execute(
                self._worker_events.insert().values(
                    hostname=event.hostname,
                    event=event.event,
                    timestamp=event.timestamp,
                    info=info_json,
                    broker_url=event.broker_url,
                ),
            )
            worker = self._worker_from_event(event)
            stmt = sqlite_insert(self._workers).values(**worker)
            update_values = dict(worker)
            update_values.pop("hostname", None)
            if event.event == "worker-offline":
                update_values["last_heartbeat"] = func.coalesce(
                    self._workers.c.last_heartbeat,
                    event.timestamp,
                )
            stmt = stmt.on_conflict_do_update(
                index_elements=[self._workers.c.hostname],
                set_=update_values,
            )
            conn.execute(stmt)

    def get_workers(self) -> list[Worker]:
        """Return all workers."""
        with self._engine.begin() as conn:
            rows = conn.execute(select(self._workers)).all()
        return [self._row_to_worker(_row_dict(row)) for row in rows]

    def get_worker(self, hostname: str) -> Worker | None:
        """Return a worker by hostname, if present."""
        stmt = select(self._workers).where(self._workers.c.hostname == hostname)
        with self._engine.begin() as conn:
            row = conn.execute(stmt).first()
        return self._row_to_worker(_row_dict(row)) if row else None

    def get_task_stats(self, task_name: str | None, time_range: TimeRange | None) -> TaskStats:
        """Compute task runtime statistics."""
        tasks = self._filter_tasks(task_name, time_range)
        runtimes = sorted([task.runtime for task in tasks if task.runtime is not None])
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
        stmt = select(self._tasks.c.state, func.count()).group_by(self._tasks.c.state)
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return {row[0]: int(row[1]) for row in rows if row[0] is not None}

    def get_heatmap(self, time_range: TimeRange | None) -> list[list[int]]:
        """Return a heatmap of task activity."""
        heatmap = [[0 for _ in range(24)] for _ in range(7)]
        tasks = self._filter_tasks(None, time_range)
        for task in tasks:
            timestamp = _task_timestamp(task)
            if timestamp is None:
                continue
            timestamp = _coerce_dt(timestamp) or timestamp
            heatmap[timestamp.weekday()][timestamp.hour] += 1
        return heatmap

    def get_schedules(self) -> list[Schedule]:
        """Return all stored schedules."""
        with self._engine.begin() as conn:
            rows = conn.execute(select(self._schedules)).all()
        return [self._row_to_schedule(_row_dict(row)) for row in rows]

    def store_schedule(self, schedule: Schedule) -> None:
        """Persist a schedule entry."""
        values = self._schedule_values(schedule)
        stmt = sqlite_insert(self._schedules).values(**values)
        update_values = dict(values)
        update_values.pop("schedule_id", None)
        stmt = stmt.on_conflict_do_update(
            index_elements=[self._schedules.c.schedule_id],
            set_=update_values,
        )
        with self._engine.begin() as conn:
            conn.execute(stmt)

    def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule entry by ID."""
        with self._engine.begin() as conn:
            conn.execute(delete(self._schedules).where(self._schedules.c.schedule_id == schedule_id))

    def cleanup(self, older_than_days: int) -> int:
        """Delete historical data older than the cutoff."""
        cutoff = _coerce_dt(datetime.now(UTC) - timedelta(days=older_than_days))
        if cutoff is None:
            return 0
        ts_col = func.coalesce(
            self._tasks.c.finished,
            self._tasks.c.started,
            self._tasks.c.received,
        )
        total_removed = 0
        with self._engine.begin() as conn:
            result = conn.execute(delete(self._task_events).where(self._task_events.c.timestamp < cutoff))
            total_removed += result.rowcount or 0
            result = conn.execute(delete(self._tasks).where(ts_col < cutoff))
            total_removed += result.rowcount or 0
            result = conn.execute(delete(self._worker_events).where(self._worker_events.c.timestamp < cutoff))
            total_removed += result.rowcount or 0
            result = conn.execute(
                delete(self._workers).where(
                    (self._workers.c.last_heartbeat.is_not(None)) & (self._workers.c.last_heartbeat < cutoff),
                ),
            )
            total_removed += result.rowcount or 0
        return total_removed

    def close(self) -> None:
        """Dispose of the SQLite engine."""
        self._engine.dispose()

    def _ensure_writable_path(self) -> None:
        if self._path.exists():
            if self._path.is_dir():
                msg = f"SQLite database path points to a directory: {self._path}"
                raise RuntimeError(msg)
            if not os.access(self._path, os.W_OK):
                msg = (
                    "SQLite database file is not writable. "
                    f"Check permissions for {self._path} or set CnCConfig(db_path=...)."
                )
                raise RuntimeError(msg)
            return

        parent = self._path.parent
        if not parent.exists():
            parent.mkdir(parents=True, exist_ok=True)
        if not os.access(parent, os.W_OK):
            msg = (
                "SQLite database directory is not writable. "
                f"Check permissions for {parent} or set CnCConfig(db_path=...)."
            )
            raise RuntimeError(msg)

    def _define_tables(self) -> None:
        self._schema_version = Table(
            "schema_version",
            self._metadata,
            Column("version", Integer, primary_key=True),
        )
        self._tasks = Table(
            "tasks",
            self._metadata,
            Column("task_id", String, primary_key=True),
            Column("name", String),
            Column("state", String, nullable=False),
            Column("worker", String),
            Column("received", DateTime(timezone=True)),
            Column("started", DateTime(timezone=True)),
            Column("finished", DateTime(timezone=True)),
            Column("runtime", Float),
            Column("args", Text),
            Column("kwargs", Text),
            Column("result", Text),
            Column("traceback", Text),
            Column("stamps", Text),
            Column("retries", Integer),
            Column("parent_id", String),
            Column("root_id", String),
            Column("group_id", String),
            Column("chord_id", String),
        )
        self._task_events = Table(
            "task_events",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("task_id", String, nullable=False),
            Column("name", String),
            Column("state", String, nullable=False),
            Column("timestamp", DateTime(timezone=True), nullable=False),
            Column("worker", String),
            Column("args", Text),
            Column("kwargs", Text),
            Column("result", Text),
            Column("traceback", Text),
            Column("stamps", Text),
            Column("runtime", Float),
            Column("retries", Integer),
            Column("eta", DateTime(timezone=True)),
            Column("expires", DateTime(timezone=True)),
            Column("parent_id", String),
            Column("root_id", String),
            Column("group_id", String),
            Column("chord_id", String),
        )
        self._task_relations = Table(
            "task_relations",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("root_id", String, nullable=False),
            Column("parent_id", String),
            Column("child_id", String, nullable=False),
            Column("relation", String, nullable=False),
        )
        self._worker_events = Table(
            "worker_events",
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("hostname", String, nullable=False),
            Column("event", String, nullable=False),
            Column("timestamp", DateTime(timezone=True), nullable=False),
            Column("info", Text),
            Column("broker_url", Text),
        )
        self._workers = Table(
            "workers",
            self._metadata,
            Column("hostname", String, primary_key=True),
            Column("status", String, nullable=False),
            Column("last_heartbeat", DateTime(timezone=True)),
            Column("pool_size", Integer),
            Column("active_tasks", Integer),
            Column("registered_tasks", Text),
            Column("queues", Text),
            Column("broker_url", Text),
        )
        self._schedules = Table(
            "schedules",
            self._metadata,
            Column("schedule_id", String, primary_key=True),
            Column("name", String, nullable=False),
            Column("task", String, nullable=False),
            Column("schedule", String, nullable=False),
            Column("args", Text),
            Column("kwargs", Text),
            Column("enabled", Boolean, nullable=False),
            Column("last_run_at", DateTime(timezone=True)),
            Column("total_run_count", Integer),
            Column("app", String),
        )

    def _event_values(self, event: TaskEvent) -> dict[str, object]:
        return {
            "task_id": event.task_id,
            "name": event.name,
            "state": event.state,
            "timestamp": event.timestamp,
            "worker": event.worker,
            "args": event.args,
            "kwargs": event.kwargs,
            "result": event.result,
            "traceback": event.traceback,
            "stamps": event.stamps,
            "runtime": event.runtime,
            "retries": event.retries,
            "eta": event.eta,
            "expires": event.expires,
            "parent_id": event.parent_id,
            "root_id": event.root_id,
            "group_id": event.group_id,
            "chord_id": event.chord_id,
        }

    def _task_values_from_event(self, event: TaskEvent, existing_state: str | None) -> dict[str, object]:
        values: dict[str, object] = {
            "task_id": event.task_id,
            "state": event.state,
        }
        preserve = existing_state is not None and self._should_preserve_state(existing_state, event.state)
        if preserve:
            values["state"] = existing_state
        else:
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
            values.update({field_name: value for field_name, value in updates.items() if value is not None})
            if event.state in {"RECEIVED", "PENDING"}:
                values["received"] = event.timestamp
            elif event.state == "STARTED":
                values["started"] = event.timestamp
            elif event.state in _FINAL_STATES:
                values["finished"] = event.timestamp
        return values

    def _get_task_state(self, conn: Connection, task_id: str) -> str | None:
        row = conn.execute(
            select(self._tasks.c.state).where(self._tasks.c.task_id == task_id),
        ).first()
        if row is None or row[0] is None:
            return None
        return str(row[0])

    @staticmethod
    def _should_preserve_state(existing_state: str, incoming_state: str) -> bool:
        if existing_state in _FINAL_STATES and incoming_state not in _FINAL_STATES:
            return True
        if existing_state == "STARTED" and incoming_state in {"PENDING", "RECEIVED"}:
            return True
        return existing_state == "RECEIVED" and incoming_state == "PENDING"

    @staticmethod
    def _row_to_task(row: Mapping[str, object]) -> Task:
        return Task(
            task_id=_as_str(row["task_id"]),
            name=_as_optional_str(row.get("name")),
            state=_as_str(row["state"]),
            worker=_as_optional_str(row.get("worker")),
            received=_coerce_dt(_as_optional_datetime(row.get("received"))),
            started=_coerce_dt(_as_optional_datetime(row.get("started"))),
            finished=_coerce_dt(_as_optional_datetime(row.get("finished"))),
            runtime=_as_optional_float(row.get("runtime")),
            args=_as_optional_str(row.get("args")),
            kwargs=_as_optional_str(row.get("kwargs")),
            result=_as_optional_str(row.get("result")),
            traceback=_as_optional_str(row.get("traceback")),
            stamps=_as_optional_str(row.get("stamps")),
            retries=_as_optional_int(row.get("retries")),
            parent_id=_as_optional_str(row.get("parent_id")),
            root_id=_as_optional_str(row.get("root_id")),
            group_id=_as_optional_str(row.get("group_id")),
            chord_id=_as_optional_str(row.get("chord_id")),
        )

    @staticmethod
    def _row_to_worker(row: Mapping[str, object]) -> Worker:
        registered = SQLiteController._parse_json_list(_as_optional_str(row.get("registered_tasks")))
        queues = SQLiteController._parse_json_list(_as_optional_str(row.get("queues")))
        return Worker(
            hostname=_as_str(row["hostname"]),
            status=_as_str(row["status"]),
            last_heartbeat=_coerce_dt(_as_optional_datetime(row.get("last_heartbeat"))),
            pool_size=_as_optional_int(row.get("pool_size")),
            active_tasks=_as_optional_int(row.get("active_tasks")),
            registered_tasks=registered,
            queues=queues,
            broker_url=_as_optional_str(row.get("broker_url")),
        )

    @staticmethod
    def _row_to_schedule(row: Mapping[str, object]) -> Schedule:
        return Schedule(
            schedule_id=_as_str(row["schedule_id"]),
            name=_as_str(row["name"]),
            task=_as_str(row["task"]),
            schedule=_as_str(row["schedule"]),
            args=_as_optional_str(row.get("args")),
            kwargs=_as_optional_str(row.get("kwargs")),
            enabled=bool(row.get("enabled")),
            last_run_at=_coerce_dt(_as_optional_datetime(row.get("last_run_at"))),
            total_run_count=_as_optional_int(row.get("total_run_count")),
            app=_as_optional_str(row.get("app")),
        )

    @staticmethod
    def _row_to_relation(row: Mapping[str, object]) -> TaskRelation:
        return TaskRelation(
            root_id=_as_str(row["root_id"]),
            parent_id=_as_optional_str(row.get("parent_id")),
            child_id=_as_str(row["child_id"]),
            relation=_as_str(row["relation"]),
        )

    @staticmethod
    def _parse_json_list(value: str | None) -> list[str] | None:
        if value is None:
            return None
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(data, list):
            return [str(item) for item in data]
        return None

    def _worker_from_event(self, event: WorkerEvent) -> dict[str, object]:
        status = "OFFLINE" if event.event == "worker-offline" else "ONLINE"
        info = event.info or {}
        pool_size = None
        active_tasks = None
        registered_tasks: list[str] | None = None
        queues: list[str] | None = None
        if isinstance(info, dict):
            active_tasks = self._parse_active_tasks(info.get("active"))
            pool_size = self._parse_pool_size(info.get("pool"))
            registered_tasks = self._parse_registered_tasks(info.get("registered"))
            queues = self._parse_queue_names(info.get("queues"))
        return {
            "hostname": event.hostname,
            "status": status,
            "last_heartbeat": event.timestamp if event.event != "worker-offline" else None,
            "pool_size": pool_size,
            "active_tasks": active_tasks,
            "registered_tasks": json.dumps(registered_tasks) if registered_tasks is not None else None,
            "queues": json.dumps(queues) if queues is not None else None,
            "broker_url": event.broker_url,
        }

    @staticmethod
    def _parse_active_tasks(value: object) -> int | None:
        if isinstance(value, list):
            return len(value)
        return None

    @staticmethod
    def _parse_pool_size(value: object) -> int | None:
        if not isinstance(value, dict):
            return None
        pool_size = value.get("max-concurrency") or value.get("max_concurrency")
        if isinstance(pool_size, int):
            return pool_size
        return None

    @staticmethod
    def _parse_registered_tasks(value: object) -> list[str] | None:
        if isinstance(value, list):
            return [str(item) for item in value]
        return None

    @staticmethod
    def _parse_queue_names(value: object) -> list[str] | None:
        if not isinstance(value, list):
            return None
        names: list[str] = []
        for entry in value:
            if isinstance(entry, dict) and "name" in entry:
                names.append(str(entry["name"]))
            elif isinstance(entry, str):
                names.append(entry)
        return names or None

    def _apply_task_filters(
        self,
        stmt: Select[tuple[object, ...]],
        filters: TaskFilter,
        ts_col: ColumnElement[object],
    ) -> Select[tuple[object, ...]]:
        if filters.task_name:
            stmt = stmt.where(self._tasks.c.name == filters.task_name)
        if filters.state:
            stmt = stmt.where(self._tasks.c.state == filters.state)
        if filters.worker:
            stmt = stmt.where(self._tasks.c.worker == filters.worker)
        if filters.group_id:
            stmt = stmt.where(self._tasks.c.group_id == filters.group_id)
        if filters.root_id:
            stmt = stmt.where(self._tasks.c.root_id == filters.root_id)
        if filters.search:
            pattern = f"%{filters.search}%"
            stmt = stmt.where(
                (self._tasks.c.name.like(pattern))
                | (self._tasks.c.task_id.like(pattern))
                | (self._tasks.c.worker.like(pattern))
                | (self._tasks.c.args.like(pattern))
                | (self._tasks.c.kwargs.like(pattern)),
            )
        if filters.time_range:
            start = filters.time_range.start
            end = filters.time_range.end
            stmt = stmt.where(ts_col.between(start, end))
        return stmt

    def _task_sort_column(
        self,
        sort_key: str | None,
        ts_col: ColumnElement[object],
    ) -> ColumnElement[object]:
        if sort_key == "state":
            return self._tasks.c.state
        if sort_key == "worker":
            return self._tasks.c.worker
        if sort_key == "received":
            return self._tasks.c.received
        if sort_key == "started":
            return self._tasks.c.started
        if sort_key == "runtime":
            return self._tasks.c.runtime
        return ts_col

    def _filter_tasks(self, task_name: str | None, time_range: TimeRange | None) -> list[Task]:
        stmt: Select[tuple[object, ...]] = select(self._tasks)
        ts_col = func.coalesce(
            self._tasks.c.finished,
            self._tasks.c.started,
            self._tasks.c.received,
        )
        if task_name is not None:
            stmt = stmt.where(self._tasks.c.name == task_name)
        if time_range is not None:
            stmt = stmt.where(ts_col.between(time_range.start, time_range.end))
        with self._engine.begin() as conn:
            rows = conn.execute(stmt).all()
        return [self._row_to_task(_row_dict(row)) for row in rows]

    @staticmethod
    def _init_buckets(time_range: TimeRange, bucket_seconds: int) -> dict[datetime, int]:
        start = _coerce_dt(time_range.start) or time_range.start
        end = _coerce_dt(time_range.end) or time_range.end
        buckets: dict[datetime, int] = {}
        cursor = start
        while cursor <= end:
            buckets[cursor] = 0
            cursor = cursor + timedelta(seconds=bucket_seconds)
        return buckets

    @staticmethod
    def _bucket_start(start: datetime, timestamp: datetime, bucket_seconds: int) -> datetime:
        start = _coerce_dt(start) or start
        timestamp = _coerce_dt(timestamp) or timestamp
        delta_seconds = int((timestamp - start).total_seconds())
        bucket_index = max(delta_seconds // bucket_seconds, 0)
        return start + timedelta(seconds=bucket_index * bucket_seconds)

    @staticmethod
    def _schedule_values(schedule: Schedule) -> dict[str, object]:
        return {
            "schedule_id": schedule.schedule_id,
            "name": schedule.name,
            "task": schedule.task,
            "schedule": schedule.schedule,
            "args": schedule.args,
            "kwargs": schedule.kwargs,
            "enabled": schedule.enabled,
            "last_run_at": schedule.last_run_at,
            "total_run_count": schedule.total_run_count,
            "app": schedule.app,
        }
