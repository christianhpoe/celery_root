# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Broker inspection helpers for Root."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Protocol, cast, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from celery import Celery

    from celery_root.core.registry import WorkerRegistry

__all__ = ["QueueInfo", "list_queues", "purge_queues"]

_INSPECT_TIMEOUT_SECONDS = 1.0


@dataclass(slots=True)
class QueueInfo:
    """Summary of a broker queue."""

    name: str
    messages: int | None
    consumers: int | None


@runtime_checkable
class _RedisClient(Protocol):
    def llen(self, key: str) -> int:
        """Return the length of the list stored at the key."""
        ...

    def delete(self, *keys: str) -> int:
        """Delete one or more keys and return count removed."""
        ...

    def scan_iter(self, match: str) -> Iterable[object]:
        """Yield keys matching a pattern."""
        ...


class _Channel(Protocol):
    def queue_declare(self, *, queue: str, passive: bool) -> object: ...
    def queue_purge(self, *, queue: str) -> object: ...


class _Transport(Protocol):
    driver_type: str | None


class _Connection(Protocol):
    transport: _Transport | None
    default_channel: object | None

    def channel(self) -> _Channel: ...


class _Inspector(Protocol):
    def active_queues(self) -> object: ...


def _get_app(registry: WorkerRegistry, worker: str) -> Celery:
    return registry.get_app(worker)


def _inspect(app: Celery) -> _Inspector | None:
    try:
        return app.control.inspect(timeout=_INSPECT_TIMEOUT_SECONDS)
    except TypeError:
        return app.control.inspect()


def list_queues(
    registry: WorkerRegistry,
    worker: str,
    pattern: str | None = None,
    *,
    connection: _Connection | None = None,
    include_counts: bool = True,
) -> list[QueueInfo]:
    """List queues and (best-effort) message counts."""
    app = _get_app(registry, worker)
    queue_names = _discover_queue_names(app)
    if pattern is not None:
        queue_names = [name for name in queue_names if fnmatch(name, pattern)]

    if not include_counts:
        return [QueueInfo(name, None, None) for name in queue_names]

    with _connection(app, connection) as conn:
        connection_obj: _Connection = conn
        return [_queue_info(app, name, connection=connection_obj) for name in queue_names]


def purge_queues(
    registry: WorkerRegistry,
    worker: str,
    queue: str | None = None,
    pattern: str | None = None,
    *,
    connection: _Connection | None = None,
) -> dict[str, int | None]:
    """Purge queues (all, single queue, or pattern)."""
    targets = (
        [queue]
        if queue is not None
        else [info.name for info in list_queues(registry, worker, pattern, connection=connection, include_counts=False)]
    )
    app = _get_app(registry, worker)
    purged: dict[str, int | None] = {}
    with _connection(app, connection) as conn:
        connection_obj: _Connection = conn
        for name in targets:
            purged[name] = _purge_queue(app, name, connection=connection_obj)
    return purged


def _discover_queue_names(app: Celery) -> list[str]:
    inspector = _inspect(app)
    queue_names: set[str] = set()
    try:
        queues = inspector.active_queues() if inspector is not None else None
    except Exception:  # noqa: BLE001 - best-effort inspection
        queues = None
    if isinstance(queues, dict):
        for queue_list in queues.values():
            for queue in queue_list:
                name = queue.get("name")
                if name:
                    queue_names.add(str(name))
    if not queue_names and getattr(app.conf, "task_queues", None):
        queue_names.update(str(q.name) for q in app.conf.task_queues if getattr(q, "name", None))
    if not queue_names:
        default_queue = getattr(app.conf, "task_default_queue", None) or "celery"
        queue_names.add(str(default_queue))
    return sorted(queue_names)


def _queue_info(app: Celery, queue: str, *, connection: _Connection) -> QueueInfo:
    try:
        return _broker_counts(app, queue, connection=connection)
    except (AttributeError, OSError, RuntimeError, TypeError, ValueError):
        return QueueInfo(queue, None, None)


def _broker_counts(_app: Celery, queue: str, *, connection: _Connection) -> QueueInfo:
    transport = connection.transport
    driver = transport.driver_type if transport is not None else None
    messages: int | None
    if driver == "redis":
        channel = connection.default_channel
        client = getattr(channel, "client", None)
        if isinstance(client, _RedisClient):
            messages = int(client.llen(queue))
            return QueueInfo(queue, messages, None)
    channel = connection.channel()
    result = channel.queue_declare(queue=queue, passive=True)
    messages = _extract_field(result, "message_count")
    consumers = _extract_field(result, "consumer_count")
    return QueueInfo(queue, messages, consumers)


def _purge_queue(_app: Celery, queue: str, *, connection: _Connection) -> int | None:
    transport = connection.transport
    driver = transport.driver_type if transport is not None else None
    if driver == "redis":
        channel = connection.default_channel
        client = getattr(channel, "client", None)
        if isinstance(client, _RedisClient):
            pending = int(client.llen(queue))
            client.delete(queue)
            return pending
    channel = connection.channel()
    result = channel.queue_purge(queue=queue)
    if isinstance(result, int):
        return result
    if isinstance(result, float | str):
        try:
            return int(result)
        except (TypeError, ValueError):
            return None
    return None


def _extract_field(result: object, name: str) -> int | None:
    if hasattr(result, name):
        try:
            return int(getattr(result, name))
        except (TypeError, ValueError):
            return None
    if isinstance(result, tuple):
        index = 1 if name == "message_count" else 2 if name == "consumer_count" else None
        if index is not None and len(result) > index:
            try:
                return int(result[index])
            except (TypeError, ValueError):
                return None
    return None


@contextmanager
def _connection(app: Celery, connection: _Connection | None) -> Iterator[_Connection]:
    if connection is not None:
        yield connection
        return
    with app.connection_or_acquire() as conn:
        yield cast("_Connection", conn)
