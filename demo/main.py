# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Launch the Celery Root supervisor with the demo workers."""

from __future__ import annotations

import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from celery_root import (
    BeatConfig,
    CeleryRoot,
    CeleryRootConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    McpConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
)
from demo.worker_math import app as math_app
from demo.worker_sleep import app as sleep_app
from demo.worker_text import app as text_app

_LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"


def _build_logger() -> logging.Logger:
    logger = logging.getLogger("demo.root")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    log_path = Path("demo_root.log")
    handler_present = False
    for handler in logger.handlers:
        if isinstance(handler, TimedRotatingFileHandler) and Path(handler.baseFilename) == log_path.resolve():
            handler_present = True
            break
    if not handler_present:
        file_handler = TimedRotatingFileHandler(
            log_path,
            when="D",
            interval=1,
            backupCount=0,
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        logger.addHandler(file_handler)
    return logger


def main() -> None:
    """Start the Root supervisor and dev web server."""
    logger = _build_logger()
    config = CeleryRootConfig(
        database=DatabaseConfigSqlite(db_path=Path("./demo/data/sqlite.db"), purge_db=True),
        open_telemetry=OpenTelemetryConfig(endpoint="http://localhost:4317"),
        beat=BeatConfig(),
        prometheus=PrometheusConfig(),
        mcp=McpConfig(port=5557, auth_key="development"),
        frontend=FrontendConfig(port=5558, host="0.0.0.0"),
    )
    root = CeleryRoot(math_app, text_app, sleep_app, config=config, logger=logger)
    root.run()


if __name__ == "__main__":
    main()
