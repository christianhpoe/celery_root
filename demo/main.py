"""Launch the Celery CnC supervisor with the demo workers."""

from __future__ import annotations

from pathlib import Path

from celery_cnc import (
    CeleryCnC,
    CeleryCnCConfig,
    DatabaseConfigSqlite,
    LoggingConfigFile,
    McpConfig,
)
from demo.worker_math import app as math_app
from demo.worker_sleep import app as sleep_app
from demo.worker_text import app as text_app


def main() -> None:
    """Start the CnC supervisor and dev web server."""
    config = CeleryCnCConfig(
        logging=LoggingConfigFile(log_dir=Path("./demo/data/logs")),
        database=DatabaseConfigSqlite(db_path=Path("./demo/data/sqlite3.db"), purge_db=False),
        mcp=McpConfig(auth_key="some-super-secret-key"),
    )
    cnc = CeleryCnC(math_app, text_app, sleep_app, config=config)
    cnc.run()


if __name__ == "__main__":
    main()
