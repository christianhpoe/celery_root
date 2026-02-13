# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Wrapper helpers for Celery inspect API."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from celery import Celery


class Inspector:
    """Small wrapper around Celery Inspector."""

    def __init__(self, app: Celery) -> None:
        """Bind the inspector client to a Celery app."""
        self._inspector = app.control.inspect()

    def raw(self) -> object:
        """Return the underlying inspector object."""
        return self._inspector
