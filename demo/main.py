"""Launch the Celery CnC supervisor with the demo workers."""

from __future__ import annotations

from pathlib import Path

from celery_cnc import CeleryCnC, CeleryCnCConfig
from demo.worker_math import app as math_app
from demo.worker_sleep import app as sleep_app
from demo.worker_text import app as text_app


def main() -> None:
    """Start the CnC supervisor and dev web server."""
    config = CeleryCnCConfig(
        log_dir=Path("./demo/data/logs"),
        db_path=Path("./demo/data/sqlite3.db"),
        purge_db=True,
        schedule_path=Path("./demo/data/schedule.beat"),
        delete_schedules_on_boot=True,
        prometheus=False,
        prometheus_path="/metrics",
        opentelemetry=False,
    )
    cnc = CeleryCnC(math_app, text_app, sleep_app, config=config)
    cnc.run()


if __name__ == "__main__":
    main()
