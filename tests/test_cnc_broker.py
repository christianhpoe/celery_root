# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from typing import Never

    from celery import Celery

from celery_root.core.engine.brokers import base as broker
from celery_root.core.registry import WorkerRegistry


class DummyRedisClient:
    def __init__(self) -> None:
        self.counts = {"celery": 3, "priority": 5}

    def llen(self, key: str) -> int:
        return self.counts.get(key, 0)

    def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            deleted += self.counts.pop(key, 0)
        return deleted

    def scan_iter(self, _match: str) -> list[str]:  # pragma: no cover - not used here
        return []


class DummyRedisTransport:
    driver_type = "redis"


class DummyRedisChannel:
    def __init__(self, client: DummyRedisClient) -> None:
        self.client = client

    def queue_declare(self, *, queue: str, passive: bool) -> object:
        _ = (queue, passive)
        return SimpleNamespace(message_count=0, consumer_count=0)

    def queue_purge(self, *, queue: str) -> object:
        _ = queue
        # pragma: no cover - redis path uses client.delete instead
        return 0


class DummyRedisConnection:
    transport: broker._Transport | None
    default_channel: object | None

    def __init__(self, client: DummyRedisClient) -> None:
        self.transport = cast("broker._Transport", DummyRedisTransport())  # noqa: SLF001
        self.default_channel = DummyRedisChannel(client)

    def channel(self) -> DummyRedisChannel:
        return cast("DummyRedisChannel", self.default_channel)


class DummyInspector:
    def active_queues(self) -> dict[str, list[dict[str, str]]]:
        return {"dummy@host": [{"name": "celery"}, {"name": "priority"}]}


class DummyControl:
    def __init__(self) -> None:
        self._inspector = DummyInspector()

    def inspect(self) -> DummyInspector:
        return self._inspector


class DummyApp:
    def __init__(self) -> None:
        self.control = DummyControl()
        self.conf = SimpleNamespace(task_default_queue="celery", broker_url="memory://")
        self.main = "dummy"

    def connection_or_acquire(self) -> Never:  # pragma: no cover - connection injected in tests
        raise NotImplementedError


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)  # noqa: SLF001
    return registry


def test_list_queues_redis_counts() -> None:
    app = DummyApp()
    registry = make_registry(app)
    connection = DummyRedisConnection(DummyRedisClient())

    infos = broker.list_queues(registry, "dummy", connection=connection)

    assert [info.name for info in infos] == ["celery", "priority"]
    assert [info.messages for info in infos] == [3, 5]


def test_purge_specific_queue() -> None:
    app = DummyApp()
    registry = make_registry(app)
    client = DummyRedisClient()
    connection = DummyRedisConnection(client)

    purged = broker.purge_queues(registry, "dummy", queue="celery", connection=connection)

    assert purged["celery"] == 3
    assert client.counts.get("celery", 0) == 0


def test_purge_by_pattern() -> None:
    app = DummyApp()
    registry = make_registry(app)
    client = DummyRedisClient()
    connection = DummyRedisConnection(client)

    purged = broker.purge_queues(registry, "dummy", pattern="prio*", connection=connection)

    assert purged == {"priority": 5}
    assert client.counts.get("priority", 0) == 0
