# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import TYPE_CHECKING, TypedDict, cast

from celery_root.core.engine import tasks
from celery_root.core.registry import WorkerRegistry

if TYPE_CHECKING:
    from celery import Celery


class _SentTask(TypedDict):
    name: str
    args: tuple[object, ...]
    kwargs: dict[str, object]
    options: dict[str, object]


class DummyControl:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...], dict[str, object]]] = []

    def revoke(
        self,
        task_id: str,
        *,
        terminate: bool = False,
        signal: str | None = None,
        destination: list[str] | None = None,
    ) -> list[str]:
        self.calls.append(
            (
                "revoke",
                (task_id,),
                {"terminate": terminate, "signal": signal, "destination": destination},
            ),
        )
        return ["revoked"]

    def rate_limit(self, task_name: str, rate: str, destination: list[str] | None = None) -> list[str]:
        self.calls.append(("rate_limit", (task_name, rate), {"destination": destination}))
        return ["limited"]

    def time_limit(
        self,
        task_name: str,
        soft: int | None = None,
        hard: int | None = None,
        destination: list[str] | None = None,
    ) -> list[str]:
        self.calls.append(("time_limit", (task_name,), {"soft": soft, "hard": hard, "destination": destination}))
        return ["timed"]


class DummyApp:
    def __init__(self) -> None:
        self.sent: _SentTask | None = None
        self.control = DummyControl()
        self.main = "dummy"
        self.conf = SimpleNamespace(broker_url="memory://")

    def send_task(
        self,
        name: str,
        args: tuple[object, ...],
        kwargs: dict[str, object],
        **options: object,
    ) -> str:
        self.sent = {
            "name": name,
            "args": args,
            "kwargs": kwargs,
            "options": dict(options),
        }
        return "sent"


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)  # noqa: SLF001
    return registry


def test_send_task_passes_through_arguments() -> None:
    app = DummyApp()
    registry = make_registry(app)

    result = tasks.send_task(
        registry,
        "dummy",
        "demo.add",
        args=[1, 2],
        kwargs={"x": 3},
        countdown=10,
        eta=datetime(2024, 1, 1, tzinfo=UTC),
        priority=5,
    )

    assert result == "sent"
    assert app.sent is not None
    assert app.sent["name"] == "demo.add"
    assert app.sent["args"] == (1, 2)
    assert app.sent["kwargs"] == {"x": 3}
    options = app.sent["options"]
    assert options["countdown"] == 10
    assert options["priority"] == 5


def test_revoke_records_call() -> None:
    app = DummyApp()
    registry = make_registry(app)

    response = tasks.revoke(
        registry,
        "dummy",
        "task-id",
        terminate=True,
        signal="SIGKILL",
        destination=["w1"],
    )

    assert response == ["revoked"]
    name, args, kwargs = app.control.calls[-1]
    assert name == "revoke"
    assert args == ("task-id",)
    assert kwargs["terminate"] is True
    assert kwargs["signal"] == "SIGKILL"
    assert kwargs["destination"] == ["w1"]


def test_rate_limit_records_call() -> None:
    app = DummyApp()
    registry = make_registry(app)

    response = tasks.rate_limit(
        registry,
        "dummy",
        "demo.add",
        "10/m",
        destination=["w2"],
    )

    assert response == ["limited"]
    name, args, kwargs = app.control.calls[-1]
    assert name == "rate_limit"
    assert args == ("demo.add", "10/m")
    assert kwargs["destination"] == ["w2"]


def test_time_limit_records_call() -> None:
    app = DummyApp()
    registry = make_registry(app)

    response = tasks.time_limit(
        registry,
        "dummy",
        "demo.add",
        soft=5,
        hard=10,
    )

    assert response == ["timed"]
    name, args, kwargs = app.control.calls[-1]
    assert name == "time_limit"
    assert args == ("demo.add",)
    assert kwargs["soft"] == 5
    assert kwargs["hard"] == 10
