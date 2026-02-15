# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from types import SimpleNamespace
from typing import TYPE_CHECKING, cast

from celery_root.core.engine import workers
from celery_root.core.registry import WorkerRegistry

if TYPE_CHECKING:
    from celery import Celery


class DummyInspector:
    def __init__(self, stats_response: dict[str, object] | None = None) -> None:
        self.stats_response = stats_response or {"dummy@host": {"pool": 4}}
        self.called = False

    def stats(self) -> dict[str, object]:
        self.called = True
        return self.stats_response


class DummyControl:
    def __init__(self, inspector: DummyInspector | None = None) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []
        self._inspector = inspector or DummyInspector()

    def pool_grow(self, n: int = 1, destination: list[str] | None = None) -> list[str]:
        self.calls.append(("pool_grow", (n,), {"destination": destination}))
        return ["grown"]

    def pool_shrink(self, n: int = 1, destination: list[str] | None = None) -> list[str]:
        self.calls.append(("pool_shrink", (n,), {"destination": destination}))
        return ["shrunk"]

    def autoscale(
        self,
        max_concurrency: int,
        min_concurrency: int = 0,
        destination: list[str] | None = None,
    ) -> list[str]:
        self.calls.append(("autoscale", (max_concurrency, min_concurrency), {"destination": destination}))
        return ["autoscaled"]

    def broadcast(
        self,
        command: str,
        arguments: dict[str, object] | None = None,
        destination: list[str] | None = None,
    ) -> list[str]:
        self.calls.append(("broadcast", (command,), {"arguments": arguments, "destination": destination}))
        return ["broadcasted"]

    def add_consumer(
        self,
        queue: str,
        exchange: str | None = None,
        routing_key: str | None = None,
        destination: list[str] | None = None,
    ) -> list[str]:
        self.calls.append(
            (
                "add_consumer",
                (queue,),
                {
                    "exchange": exchange,
                    "routing_key": routing_key,
                    "destination": destination,
                },
            ),
        )
        return ["added"]

    def cancel_consumer(self, queue: str, destination: list[str] | None = None) -> list[str]:
        self.calls.append(("cancel_consumer", (queue,), {"destination": destination}))
        return ["cancelled"]

    def inspect(self, destination: list[str] | None = None) -> DummyInspector:
        _ = destination
        return self._inspector


class DummyApp:
    def __init__(self) -> None:
        self.control = DummyControl()
        self.main = "dummy"
        self.conf = SimpleNamespace(broker_url="memory://")


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)
    return registry


def test_pool_grow_and_shrink() -> None:
    app = DummyApp()
    registry = make_registry(app)

    grow_result = workers.pool_grow(registry, "dummy", amount=2, destination=["w1"])
    shrink_result = workers.pool_shrink(registry, "dummy", amount=1)

    assert grow_result == ["grown"]
    assert shrink_result == ["shrunk"]
    assert ("pool_grow", (2,), {"destination": ["w1"]}) in app.control.calls
    assert ("pool_shrink", (1,), {"destination": None}) in app.control.calls


def test_autoscale_and_restart_shutdown() -> None:
    app = DummyApp()
    registry = make_registry(app)

    auto_result = workers.autoscale(
        registry,
        "dummy",
        max_concurrency=10,
        min_concurrency=3,
    )
    shutdown_result = workers.shutdown(registry, "dummy", destination=["w1"])
    restart_result = workers.restart(registry, "dummy", reload=True)

    assert auto_result == ["autoscaled"]
    assert shutdown_result == ["broadcasted"]
    assert restart_result == ["broadcasted"]

    commands = [call[0] for call in app.control.calls]
    assert "autoscale" in commands
    assert (
        "broadcast",
        ("shutdown",),
        {"arguments": None, "destination": ["w1"]},
    ) in app.control.calls
    assert (
        "broadcast",
        ("restart",),
        {"arguments": {"reload": True}, "destination": None},
    ) in app.control.calls


def test_consumer_management() -> None:
    app = DummyApp()
    registry = make_registry(app)

    add_result = workers.add_consumer(
        registry,
        "dummy",
        "queue-a",
        exchange="ex",
        routing_key="rk",
    )
    remove_result = workers.remove_consumer(registry, "dummy", "queue-a", destination=["w2"])

    assert add_result == ["added"]
    assert remove_result == ["cancelled"]
    assert (
        "add_consumer",
        ("queue-a",),
        {"exchange": "ex", "routing_key": "rk", "destination": None},
    ) in app.control.calls
    assert ("cancel_consumer", ("queue-a",), {"destination": ["w2"]}) in app.control.calls


def test_get_stats_uses_inspector() -> None:
    inspector = DummyInspector(stats_response={"dummy@host": {"active": 1}})
    app = DummyApp()
    app.control = DummyControl(inspector=inspector)
    registry = make_registry(app)

    stats = workers.get_stats(registry, "dummy")

    assert stats == {"dummy@host": {"active": 1}}
    assert inspector.called is True
