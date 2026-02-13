# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Shared runtime context passed to components."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable

    from celery_root.config import CeleryRootConfig
    from celery_root.core.db.adapters.base import BaseDBController
    from celery_root.core.registry import WorkerRegistry


@dataclass(slots=True)
class ComponentContext:
    """Runtime context shared with components."""

    config: CeleryRootConfig
    registry: WorkerRegistry
    db_factory: Callable[[], BaseDBController]
    logger: logging.Logger
