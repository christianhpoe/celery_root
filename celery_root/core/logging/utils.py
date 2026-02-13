# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Shared helpers for log file naming and levels."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path
LOG_FILE_PREFIX = "celery_root"
LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"

_COMPONENT_RE = re.compile(r"[^a-zA-Z0-9._-]+")


def sanitize_component(component: str) -> str:
    """Return a filesystem-safe component identifier."""
    cleaned = _COMPONENT_RE.sub("_", component).strip("._-")
    return cleaned or "app"


def log_file_name(component: str | None) -> str:
    """Return the log file name for a component."""
    if component:
        return f"{LOG_FILE_PREFIX}-{sanitize_component(component)}.log"
    return f"{LOG_FILE_PREFIX}.log"


def log_file_path(log_dir: Path, component: str | None) -> Path:
    """Return the full log path for a component."""
    return log_dir / log_file_name(component)


def resolve_log_level(value: str | int | None) -> int:
    """Normalize a log level string/int to logging's numeric constant."""
    if isinstance(value, int):
        return value
    if value is None:
        return logging.INFO
    if isinstance(value, str):
        normalized = value.strip().upper()
        if not normalized:
            return logging.INFO
        if normalized.isdigit():
            return int(normalized)
        resolved = logging.getLevelName(normalized)
        if isinstance(resolved, int):
            return resolved
    return logging.INFO
