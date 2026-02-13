# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Wrapper helpers for Celery control API."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery import Celery


class Control:
    """Small wrapper around Celery Control API (stub)."""

    def __init__(self, app: Celery) -> None:
        """Bind the control client to a Celery app."""
        self._control = app.control

    def raw(self) -> object:
        """Return the underlying Celery control client."""
        return self._control
