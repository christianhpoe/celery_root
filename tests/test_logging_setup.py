# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, LoggingConfigFile
from celery_root.core.logging import setup as logging_setup
from celery_root.core.logging.utils import log_file_path, resolve_log_level


def test_find_handler_matches_log_file(tmp_path: Path) -> None:
    logger = logging.getLogger("celery_root.tests.find_handler")
    log_file = tmp_path / "app.log"
    handler = TimedRotatingFileHandler(log_file)
    logger.addHandler(handler)
    try:
        found = logging_setup._find_handler(logger, log_file)
        assert found is handler
        assert logging_setup._find_handler(logger, tmp_path / "other.log") is None
    finally:
        logger.removeHandler(handler)
        handler.close()


def test_configure_process_logging_is_idempotent(tmp_path: Path) -> None:
    root_logger = logging.getLogger()
    original_handlers = list(root_logger.handlers)
    original_level = root_logger.level
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs", log_level="WARNING", log_rotation_hours=1),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
    )
    log_file = log_file_path(Path(config.logging.log_dir), "worker")
    try:
        logging_setup.configure_process_logging(config, component="worker")
        handler = logging_setup._find_handler(root_logger, log_file)
        assert handler is not None
        expected_level = resolve_log_level(config.logging.log_level)
        assert root_logger.level == expected_level
        assert handler.level == expected_level

        logging_setup.configure_process_logging(config, component="worker")
        handler_after = logging_setup._find_handler(root_logger, log_file)
        assert handler_after is handler
    finally:
        for extra_handler in list(root_logger.handlers):
            if extra_handler not in original_handlers:
                root_logger.removeHandler(extra_handler)
                extra_handler.close()
        root_logger.setLevel(original_level)
