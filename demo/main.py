# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Launch the Celery Root supervisor with the demo workers."""

from __future__ import annotations

from pathlib import Path

from celery_root import (
    BeatConfig,
    CeleryRoot,
    CeleryRootConfig,
    DatabaseConfigSqlite,
    LoggingConfigFile,
    McpConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
)
from demo.worker_math import app as math_app
from demo.worker_sleep import app as sleep_app
from demo.worker_text import app as text_app


def main() -> None:
    """Start the Root supervisor and dev web server."""
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=Path("./demo/data/logs"), delete_on_boot=True),
        database=DatabaseConfigSqlite(db_path=Path("./demo/data/sqlite3.db"), purge_db=True),
        open_telemetry=OpenTelemetryConfig(endpoint="http://localhost:4317"),
        beat=BeatConfig(schedule_path=Path("./demo/data/beat.schedule"), delete_schedules_on_boot=True),
        prometheus=PrometheusConfig(),
        mcp=McpConfig(port=9100, auth_key="some-super-secret-key"),
    )
    root = CeleryRoot(math_app, text_app, sleep_app, config=config)
    root.run()


if __name__ == "__main__":
    main()
