# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Component interfaces for Celery Root."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass(slots=True)
class ComponentStatus:
    """Lightweight component status snapshot."""

    name: str
    enabled: bool
    up: bool
    details: dict[str, object] = field(default_factory=dict)


class BaseComponent(ABC):
    """Base interface for optional runtime components."""

    name: str
    enabled: bool

    @abstractmethod
    def start(self, ctx: object) -> None:
        """Start the component."""
        ...

    @abstractmethod
    def stop(self, ctx: object) -> None:
        """Stop the component."""
        ...

    @abstractmethod
    def status(self) -> ComponentStatus:
        """Return the component's current status."""
        ...
