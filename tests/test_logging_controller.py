from __future__ import annotations

from typing import TYPE_CHECKING

from celery_root.config import LoggingConfigFile
from celery_root.core.logging.adapters.file import FileLogController

if TYPE_CHECKING:
    from pathlib import Path


def test_file_log_controller_writes_rotating_file(tmp_path: Path) -> None:
    config = LoggingConfigFile(log_dir=tmp_path, log_rotation_hours=1)
    controller = FileLogController()

    controller.configure(config)
    logger = controller.get_logger("root-test")
    logger.info("hello log")
    controller.shutdown()

    log_files = list(tmp_path.glob("celery_root-root-test.log*"))
    assert log_files, "log file should be created"
    content = log_files[0].read_text(encoding="utf-8")
    assert "hello log" in content


def test_logging_config_delete_on_boot_clears_directory(tmp_path: Path) -> None:
    (tmp_path / "old.log").write_text("stale", encoding="utf-8")
    nested = tmp_path / "nested"
    nested.mkdir()
    (nested / "nested.log").write_text("stale", encoding="utf-8")

    LoggingConfigFile(log_dir=tmp_path, delete_on_boot=True)

    assert not list(tmp_path.iterdir())
