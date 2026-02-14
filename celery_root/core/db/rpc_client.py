# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""RPC client for DB manager operations."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing.connection import Client, Connection
from typing import TYPE_CHECKING, Any, Self, TypeVar, cast

from pydantic import BaseModel, ValidationError

from celery_root.core.db.adapters.base import BaseDBController
from celery_root.shared.schemas import (
    CleanupRequest,
    CleanupResponse,
    DeleteScheduleRequest,
    GetTaskRequest,
    GetTaskResponse,
    GetWorkerRequest,
    GetWorkerResponse,
    HeatmapRequest,
    HeatmapResponse,
    IngestTaskEventRequest,
    IngestWorkerEventRequest,
    ListSchedulesRequest,
    ListSchedulesResponse,
    ListTaskNamesRequest,
    ListTaskNamesResponse,
    ListTaskRelationsRequest,
    ListTaskRelationsResponse,
    ListTasksPageRequest,
    ListTasksPageResponse,
    ListTasksRequest,
    ListTasksResponse,
    ListWorkersRequest,
    ListWorkersResponse,
    Ok,
    PingRequest,
    PingResponse,
    RpcError,
    RpcRequestEnvelope,
    RpcResponseEnvelope,
    SchemaRequest,
    SchemaResponse,
    SchemaVersionRequest,
    SchemaVersionResponse,
    StateDistributionRequest,
    StateDistributionResponse,
    StoreScheduleRequest,
    StoreTaskRelationRequest,
    TaskStatsRequest,
    TaskStatsResponse,
    ThroughputRequest,
    ThroughputResponse,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from celery_root.config import CeleryRootConfig
    from celery_root.shared.schemas.domain import (
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

ReqT = TypeVar("ReqT", bound=BaseModel)
ResT = TypeVar("ResT", bound=BaseModel)

_LOGGER = logging.getLogger(__name__)


class RpcCallError(RuntimeError):
    """Raised when the RPC response indicates an error."""

    def __init__(self, error: RpcError) -> None:
        """Initialize with the error payload."""
        super().__init__(f"{error.code}: {error.message}")
        self.error = error


def _authkey_from_config(config: CeleryRootConfig) -> bytes | None:
    auth = config.database.rpc_auth_key
    if not auth:
        return None
    return auth.encode("utf-8")


@dataclass(slots=True)
class _RpcSettings:
    address: tuple[str, int]
    authkey: bytes | None
    timeout_seconds: float
    max_message_bytes: int


class _RpcTransport:
    """Low-level transport for DB RPC calls (not thread-safe)."""

    def __init__(self, settings: _RpcSettings, client_name: str | None) -> None:
        self._settings = settings
        self._client_name = client_name
        self._connection: Connection | None = None

    def connect(self) -> None:
        if self._connection is not None:
            return
        self._connection = Client(self._settings.address, authkey=self._settings.authkey)

    def close(self) -> None:
        if self._connection is None:
            return
        self._connection.close()
        self._connection = None

    def request(
        self,
        op: str,
        payload: Mapping[str, Any] | None,
        *,
        timeout_seconds: float | None = None,
        max_retries: int = 0,
    ) -> RpcResponseEnvelope:
        request_id = uuid.uuid4().hex
        envelope = RpcRequestEnvelope(
            request_id=request_id,
            op=op,
            payload=dict(payload) if payload is not None else None,
            timestamp=datetime.now(UTC),
            client=self._client_name,
        )
        data = envelope.model_dump_json().encode("utf-8")
        _LOGGER.debug(
            "RPC request start op=%s request_id=%s bytes=%d timeout=%.2f retries=%d",
            op,
            request_id,
            len(data),
            timeout_seconds or self._settings.timeout_seconds,
            max_retries,
        )
        if len(data) > self._settings.max_message_bytes:
            msg = f"RPC request too large ({len(data)} bytes)"
            raise ValueError(msg)

        timeout = timeout_seconds or self._settings.timeout_seconds
        attempts = max_retries + 1
        last_error: Exception | None = None
        for _ in range(attempts):
            try:
                self.connect()
                if self._connection is None:
                    msg = "RPC connection unavailable"
                    raise RuntimeError(msg)
                self._connection.send_bytes(data)
                poll_ok = self._connection.poll(timeout)
                if not poll_ok:
                    msg = "RPC response timed out"
                    last_error = TimeoutError(msg)
                    self.close()
                    continue
                resp_bytes = self._connection.recv_bytes()
                if len(resp_bytes) > self._settings.max_message_bytes:
                    msg = f"RPC response too large ({len(resp_bytes)} bytes)"
                    raise ValueError(msg)
                response = RpcResponseEnvelope.model_validate_json(resp_bytes)
                _LOGGER.debug(
                    "RPC response recv op=%s request_id=%s ok=%s bytes=%d",
                    op,
                    response.request_id,
                    response.ok,
                    len(resp_bytes),
                )
                if response.schema_version != envelope.schema_version:
                    msg = "RPC schema version mismatch"
                    raise RuntimeError(msg)
                if response.request_id != request_id:
                    msg = "RPC response request_id mismatch"
                    raise RuntimeError(msg)
            except (OSError, EOFError, ValidationError) as exc:  # pragma: no cover - network dependent
                last_error = exc
                self.close()
                continue
            else:
                return response
        msg = "RPC request failed"
        raise RuntimeError(msg) from last_error


class DbRpcClient(BaseDBController):
    """RPC-backed DB client implementing the DB controller interface."""

    def __init__(self, settings: _RpcSettings, *, client_name: str | None = None) -> None:
        """Initialize the RPC client."""
        self._transport = _RpcTransport(settings, client_name)

    @classmethod
    def from_config(cls, config: CeleryRootConfig, *, client_name: str | None = None) -> DbRpcClient:
        """Create a client from shared configuration settings."""
        settings = _RpcSettings(
            address=(config.database.rpc_host, config.database.rpc_port),
            authkey=_authkey_from_config(config),
            timeout_seconds=config.database.rpc_timeout_seconds,
            max_message_bytes=config.database.rpc_max_message_bytes,
        )
        return cls(settings, client_name=client_name)

    def connect(self) -> None:
        """Open the RPC connection."""
        self._transport.connect()

    def close(self) -> None:
        """Close the RPC connection."""
        self._transport.close()

    def ping(self) -> PingResponse:
        """Return the DB manager health response."""
        return self._call("db.ping", PingRequest(), PingResponse)

    def initialize(self) -> None:
        """Verify connectivity by pinging the DB manager."""
        _ = self.ping()

    def get_schema_version(self) -> int:
        """Return the remote schema version."""
        response = self._call("db.schema_version", SchemaVersionRequest(), SchemaVersionResponse)
        return response.version

    def ensure_schema(self) -> None:
        """Ensure the remote schema is available."""
        _ = self.get_schema_version()

    def get_schema(self) -> SchemaResponse:
        """Fetch database schema metadata."""
        return self._call("db.schema", SchemaRequest(), SchemaResponse)

    def migrate(self, _from_version: int, _to_version: int) -> None:
        """Migrations must be performed by the DB manager."""
        msg = "RPC clients cannot trigger migrations"
        raise RuntimeError(msg)

    def store_task_event(self, event: TaskEvent) -> None:
        """Persist a task event via RPC."""
        _ = self._call("events.task.ingest", IngestTaskEventRequest(event=event), Ok)

    def get_tasks(self, filters: TaskFilter | None = None) -> list[Task]:
        """Return tasks matching optional filters."""
        response = self._call("tasks.list", ListTasksRequest(filters=filters), ListTasksResponse)
        return response.tasks

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
        request = ListTasksPageRequest(
            filters=filters,
            sort_key=sort_key,
            sort_dir=sort_dir,
            limit=limit,
            offset=offset,
        )
        response = self._call("tasks.page", request, ListTasksPageResponse)
        return response.tasks, response.total

    def list_task_names(self) -> list[str]:
        """Return distinct task names stored in the DB."""
        response = self._call("tasks.names", ListTaskNamesRequest(), ListTaskNamesResponse)
        return response.names

    def get_task(self, task_id: str) -> Task | None:
        """Return a task by ID, if present."""
        response = self._call("tasks.get", GetTaskRequest(task_id=task_id), GetTaskResponse)
        return response.task

    def store_task_relation(self, relation: TaskRelation) -> None:
        """Persist a task relation edge."""
        _ = self._call("relations.store", StoreTaskRelationRequest(relation=relation), Ok)

    def get_task_relations(self, root_id: str) -> list[TaskRelation]:
        """Return task relations for a root task."""
        response = self._call(
            "relations.list",
            ListTaskRelationsRequest(root_id=root_id),
            ListTaskRelationsResponse,
        )
        return response.relations

    def store_worker_event(self, event: WorkerEvent) -> None:
        """Persist a worker event."""
        _ = self._call("events.worker.ingest", IngestWorkerEventRequest(event=event), Ok)

    def get_workers(self) -> list[Worker]:
        """Return all known workers."""
        response = self._call("workers.list", ListWorkersRequest(), ListWorkersResponse)
        return response.workers

    def get_worker(self, hostname: str) -> Worker | None:
        """Return a worker by hostname, if present."""
        response = self._call("workers.get", GetWorkerRequest(hostname=hostname), GetWorkerResponse)
        return response.worker

    def get_task_stats(self, task_name: str | None, time_range: TimeRange | None) -> TaskStats:
        """Return aggregated task statistics."""
        response = self._call(
            "stats.task",
            TaskStatsRequest(task_name=task_name, time_range=time_range),
            TaskStatsResponse,
        )
        return response.stats

    def get_throughput(self, time_range: TimeRange, bucket_seconds: int) -> list[ThroughputBucket]:
        """Return throughput buckets for the time range."""
        response = self._call(
            "stats.throughput",
            ThroughputRequest(time_range=time_range, bucket_seconds=bucket_seconds),
            ThroughputResponse,
        )
        return response.buckets

    def get_state_distribution(self) -> dict[str, int]:
        """Return counts by task state."""
        response = self._call(
            "stats.state_distribution",
            StateDistributionRequest(),
            StateDistributionResponse,
        )
        return response.counts

    def get_heatmap(self, time_range: TimeRange | None) -> list[list[int]]:
        """Return a heatmap of task activity."""
        response = self._call("stats.heatmap", HeatmapRequest(time_range=time_range), HeatmapResponse)
        return response.heatmap

    def get_schedules(self) -> list[Schedule]:
        """Return all stored schedules."""
        response = self._call("schedules.list", ListSchedulesRequest(), ListSchedulesResponse)
        return response.schedules

    def store_schedule(self, schedule: Schedule) -> None:
        """Persist a schedule entry."""
        _ = self._call("schedules.store", StoreScheduleRequest(schedule=schedule), Ok)

    def delete_schedule(self, schedule_id: str) -> None:
        """Delete a schedule entry by ID."""
        _ = self._call("schedules.delete", DeleteScheduleRequest(schedule_id=schedule_id), Ok)

    def cleanup(self, older_than_days: int) -> int:
        """Delete historical data older than the retention window."""
        response = self._call("db.cleanup", CleanupRequest(older_than_days=older_than_days), CleanupResponse)
        return response.removed

    def __enter__(self) -> Self:
        """Enter the context manager and connect."""
        self.connect()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Exit the context manager and close the connection."""
        self.close()

    def _call(
        self,
        op: str,
        request: ReqT,
        response_model: type[ResT],
        *,
        timeout_seconds: float | None = None,
        max_retries: int = 0,
    ) -> ResT:
        payload = request.model_dump(mode="json")
        response = self._transport.request(
            op,
            payload,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        if not response.ok:
            error = response.error or RpcError(code="UNKNOWN", message="RPC failed")
            _LOGGER.debug(
                "RPC call error op=%s code=%s message=%s",
                op,
                error.code,
                error.message,
            )
            raise RpcCallError(error)
        if response.payload is None:
            return response_model()
        return response_model.model_validate(cast("Mapping[str, Any]", response.payload))
