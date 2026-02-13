# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Logging setup helpers for subprocesses."""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from celery_root.config import CeleryRootConfig, LoggingConfigFile, get_settings
from celery_root.core.logging.utils import LOG_FORMAT, log_file_path, resolve_log_level


def _find_handler(logger: logging.Logger, log_file: Path) -> TimedRotatingFileHandler | None:
    log_path = str(log_file.resolve())
    for handler in logger.handlers:
        if not isinstance(handler, TimedRotatingFileHandler):
            continue
        base = getattr(handler, "baseFilename", None)
        if base is not None and str(base) == log_path:
            return handler
    return None


def configure_process_logging(
    config: CeleryRootConfig | LoggingConfigFile | None = None,
    *,
    component: str | None = None,
) -> None:
    """Ensure log handlers are configured for the current process."""
    resolved = config or get_settings()
    log_config = resolved.logging if isinstance(resolved, CeleryRootConfig) else resolved
    log_dir = Path(log_config.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_file_path(log_dir, component)
    rotation_hours = int(log_config.log_rotation_hours)
    log_level = resolve_log_level(log_config.log_level)
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    handler = _find_handler(root_logger, log_file)
    if handler is None:
        handler = TimedRotatingFileHandler(
            log_file,
            when="h",
            interval=rotation_hours,
            backupCount=7,
        )
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        root_logger.addHandler(handler)
    handler.setLevel(log_level)
