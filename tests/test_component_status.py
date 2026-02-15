# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from typing import TYPE_CHECKING

from celery_root.core.component_status import (
    ComponentStatus,
    ComponentStatusStore,
    _coerce_details,
    _coerce_float,
    _coerce_int,
    build_statuses,
)

if TYPE_CHECKING:
    from pathlib import Path


class _DummyProcess:
    def __init__(self, pid: int, *, alive: bool) -> None:
        self.pid = pid
        self._alive = alive

    def is_alive(self) -> bool:
        return self._alive


def test_coerce_helpers() -> None:
    assert _coerce_int("10") == 10
    assert _coerce_int("bad") is None
    assert _coerce_float("2.5") == 2.5
    assert _coerce_float("bad") == 0.0
    assert _coerce_details({"a": 1}) == {"a": 1}


def test_status_store_roundtrip(tmp_path: Path) -> None:
    path = tmp_path / "status.json"
    store = ComponentStatusStore(path)
    statuses = {"demo": ComponentStatus(name="demo", pid=123, alive=True, updated_at=1.0)}
    store.write(statuses)
    loaded = store.read()
    assert loaded["demo"].pid == 123


def test_build_statuses() -> None:
    processes = {"a": _DummyProcess(1, alive=True), "b": _DummyProcess(2, alive=False)}
    statuses = build_statuses(processes)
    assert statuses["a"].alive is True
    assert statuses["b"].alive is False
