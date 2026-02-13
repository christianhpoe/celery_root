# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Shared component status storage helpers."""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

    from celery_root.config import CeleryRootConfig


_STATUS_FILENAME = "component_status.json"


@dataclass(slots=True)
class ComponentStatus:
    """Snapshot of a component's runtime status."""

    name: str
    pid: int | None
    alive: bool
    updated_at: float
    details: dict[str, object] = field(default_factory=dict)

    def to_payload(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""
        return {
            "pid": self.pid,
            "alive": self.alive,
            "updated_at": self.updated_at,
            "details": self.details,
        }


class ComponentStatusStore:
    """Persist component status snapshots to a shared JSON file."""

    def __init__(self, path: Path) -> None:
        """Create a status store at the given path."""
        self._path = path

    @classmethod
    def from_config(cls, config: CeleryRootConfig) -> ComponentStatusStore:
        """Build a status store using the configured log directory."""
        log_dir = config.logging.log_dir
        return cls(log_dir / _STATUS_FILENAME)

    def read(self) -> dict[str, ComponentStatus]:
        """Read status snapshots from disk."""
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, dict):
            return {}
        statuses: dict[str, ComponentStatus] = {}
        for name, payload in raw.items():
            if not isinstance(payload, dict):
                continue
            statuses[str(name)] = ComponentStatus(
                name=str(name),
                pid=_coerce_int(payload.get("pid")),
                alive=bool(payload.get("alive", False)),
                updated_at=_coerce_float(payload.get("updated_at")),
                details=_coerce_details(payload.get("details")),
            )
        return statuses

    def write(self, statuses: Mapping[str, ComponentStatus]) -> None:
        """Persist status snapshots to disk."""
        payload = {name: status.to_payload() for name, status in statuses.items()}
        tmp_path = self._path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self._path)


def is_pid_alive(pid: int | None) -> bool:
    """Return True if the PID appears to be alive on this host."""
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def build_statuses(processes: Mapping[str, object]) -> dict[str, ComponentStatus]:
    """Build status entries for process handles."""
    now = time.time()
    statuses: dict[str, ComponentStatus] = {}
    for name, process in processes.items():
        pid = getattr(process, "pid", None)
        alive = bool(getattr(process, "is_alive", lambda: False)())
        statuses[name] = ComponentStatus(name=str(name), pid=pid, alive=alive, updated_at=now)
    return statuses


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int | float):
        return int(value)
    if isinstance(value, (str, bytes, bytearray)):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _coerce_float(value: object) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, (str, bytes, bytearray)):
        try:
            return float(value)
        except ValueError:
            return 0.0
    return 0.0


def _coerce_details(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return {str(k): v for k, v in value.items()}
    return {}
