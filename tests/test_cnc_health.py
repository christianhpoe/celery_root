# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from types import SimpleNamespace, TracebackType
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from celery import Celery

from celery_root.core.engine import health
from celery_root.core.registry import WorkerRegistry


class DummyConnection:
    def __init__(self) -> None:
        self.connected = False

    def connect(self) -> None:
        self.connected = True

    def release(self) -> None:
        self.connected = False


class DummyBackend:
    def __init__(self) -> None:
        self.calls = 0

    def get_task_meta(self, task_id: str) -> dict[str, Any]:
        self.calls += 1
        return {"id": task_id}


class DummyControl:
    def __init__(self) -> None:
        self._responses = [{"dummy@host": "pong"}]
        self.ping_called = False

    def ping(self, timeout: float = 1.0) -> list[dict[str, str]]:
        _ = timeout
        self.ping_called = True
        return self._responses


class DummyApp:
    def __init__(self) -> None:
        self._connection = DummyConnection()
        self.backend = DummyBackend()
        self.control = DummyControl()
        self.conf = SimpleNamespace(broker_url="memory://")
        self.main = "dummy"

    @property
    def connection(self) -> DummyConnection:
        return self._connection

    def connection_or_acquire(self) -> AbstractContextManager[DummyConnection]:
        app = self

        class _Ctx:
            def __enter__(self) -> DummyConnection:
                return app.connection

            def __exit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                tb: TracebackType | None,
            ) -> None:
                app.connection.release()

        return _Ctx()


def make_registry(app: DummyApp) -> WorkerRegistry:
    registry = WorkerRegistry()
    registry._apps["dummy"] = cast("Celery", app)  # noqa: SLF001
    return registry


def test_health_check_reports_ok() -> None:
    app = DummyApp()
    registry = make_registry(app)

    result = health.health_check(registry, "dummy", connection=app.connection)

    broker = cast("dict[str, object]", result["broker"])
    backend = cast("dict[str, object]", result["backend"])
    workers = cast("dict[str, object]", result["workers"])
    assert broker["ok"] is True
    assert backend["ok"] is True
    assert workers["ok"] is True
    assert app.control.ping_called is True
