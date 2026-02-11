"""Public package entrypoints for Celery CnC."""

from __future__ import annotations

import functools
import importlib
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

from .config import (
    BeatConfig,
    CeleryCnCConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    LoggingConfigFile,
    McpConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
    get_settings,
    set_settings,
)
from .core.db.adapters.base import BaseDBController
from .core.db.adapters.memory import MemoryController
from .core.db.adapters.sqlite import SQLiteController
from .core.process_manager import ProcessManager
from .core.registry import WorkerRegistry

if TYPE_CHECKING:
    from collections.abc import Callable
    from types import ModuleType

    from celery import Celery


_RETENTION_ARG_POSITIVE_ERROR = "retention_days must be positive"


class CeleryCnC:
    """Bootstrap class for Celery Command & Control."""

    def __init__(
        self,
        *workers: Celery | str,
        config: CeleryCnCConfig | None = None,
        db_controller: BaseDBController | Callable[[], BaseDBController] | None = None,
        purge_db: bool | None = None,
        retention_days: int | None = None,
    ) -> None:
        """Initialize the CnC service with worker targets and configuration."""
        if config is None:
            config = get_settings()
        if retention_days is not None:
            if retention_days <= 0:
                raise ValueError(_RETENTION_ARG_POSITIVE_ERROR)
            config = config.model_copy(
                update={
                    "database": config.database.model_copy(
                        update={"retention_days": retention_days},
                    ),
                },
            )
        if purge_db is not None:
            config = config.model_copy(
                update={
                    "database": config.database.model_copy(
                        update={"purge_db": purge_db},
                    ),
                },
            )

        config = self._ensure_worker_import_paths(config, workers)
        set_settings(config)
        self.config = config
        self.registry = WorkerRegistry(workers)
        self._db_controller = db_controller
        self._process_manager: ProcessManager | None = None
        self._ensure_sqlite_db_path()
        if self.config.database.purge_db:
            self._purge_existing_db()
        if self.config.beat is not None and self.config.beat.delete_schedules_on_boot:
            self._purge_schedule_file()

    def run(self) -> None:
        """Start all subprocesses and block until shutdown."""
        controller_factory = self._resolve_db_controller_factory()
        manager = ProcessManager(self.registry, self.config, controller_factory)
        self._process_manager = manager
        manager.run()

    def _resolve_db_controller_factory(self) -> Callable[[], BaseDBController]:
        if self._db_controller is not None and callable(self._db_controller):
            return self._db_controller
        if isinstance(self._db_controller, SQLiteController):
            path = getattr(self._db_controller, "_path", self.config.database.db_path)
            return functools.partial(_make_sqlite_controller, path)
        if isinstance(self._db_controller, MemoryController):
            return functools.partial(_return_controller, self._db_controller)
        if isinstance(self._db_controller, BaseDBController):
            controller = self._db_controller
            return functools.partial(_return_controller, controller)
        return functools.partial(_make_sqlite_controller, self.config.database.db_path)

    def _ensure_sqlite_db_path(self) -> None:
        controller = self._db_controller
        if controller is not None:
            if isinstance(controller, SQLiteController):
                raw_path = getattr(controller, "_path", None)
                if raw_path is not None:
                    self.config.database.db_path = Path(raw_path).expanduser().resolve()
                return
            if isinstance(controller, BaseDBController) or callable(controller):
                return

        path = Path(self.config.database.db_path).expanduser()
        path = (Path.cwd() / path).resolve() if not path.is_absolute() else path.resolve()
        self.config.database.db_path = path

    def _purge_existing_db(self) -> None:
        path = self._resolve_purge_path()
        if path is None:
            return
        resolved = Path(path).expanduser().resolve()
        if not resolved.exists():
            return
        if resolved.is_dir():
            msg = f"SQLite database path points to a directory: {resolved}"
            raise RuntimeError(msg)
        try:
            resolved.unlink()
        except OSError as exc:  # pragma: no cover - depends on OS permissions
            msg = f"Failed to purge SQLite database at {resolved}: {exc}"
            raise RuntimeError(msg) from exc

    def _resolve_purge_path(self) -> Path | None:
        controller = self._db_controller
        if controller is None:
            return self.config.database.db_path
        if isinstance(controller, SQLiteController):
            raw_path = getattr(controller, "_path", None)
            return Path(raw_path) if raw_path is not None else self.config.database.db_path
        return None

    def _purge_schedule_file(self) -> None:
        beat_config = self.config.beat
        if beat_config is None:
            return
        schedule_path = beat_config.schedule_path
        if schedule_path is None:
            return
        resolved = Path(schedule_path).expanduser().resolve()
        if not resolved.exists():
            return
        if resolved.is_dir():
            msg = f"Beat schedule path points to a directory: {resolved}"
            raise RuntimeError(msg)
        try:
            resolved.unlink()
        except OSError as exc:  # pragma: no cover - depends on OS permissions
            msg = f"Failed to purge beat schedule file at {resolved}: {exc}"
            raise RuntimeError(msg) from exc

    @staticmethod
    def _ensure_worker_import_paths(
        config: CeleryCnCConfig,
        workers: tuple[Celery | str, ...],
    ) -> CeleryCnCConfig:
        if config.worker_import_paths:
            return config
        paths = _parse_worker_paths(os.getenv("CELERY_CNC_WORKERS"))
        if not paths:
            paths = _derive_worker_import_paths(workers)
        if not paths:
            return config
        return config.model_copy(update={"worker_import_paths": paths})


def _make_sqlite_controller(path: Path) -> BaseDBController:
    """Create a SQLite controller; multiprocessing-safe factory."""
    return SQLiteController(path)


def _return_controller(controller: BaseDBController) -> BaseDBController:
    """Return the provided controller instance."""
    return controller


def _parse_worker_paths(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


def _derive_worker_import_paths(workers: tuple[Celery | str, ...]) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for worker in workers:
        if isinstance(worker, str):
            if worker and worker not in seen:
                seen.add(worker)
                paths.append(worker)
            continue
        path = _resolve_app_import_path(worker)
        if path and path not in seen:
            seen.add(path)
            paths.append(path)
    return paths


def _resolve_app_import_path(app: Celery) -> str | None:
    candidates: list[str] = []
    raw_main = getattr(app, "main", None)
    if raw_main:
        candidates.append(str(raw_main))
    conf_main = app.conf.get("main") if getattr(app, "conf", None) else None
    if conf_main and str(conf_main) not in candidates:
        candidates.append(str(conf_main))
    for module_name in candidates:
        module = _load_module(module_name)
        if module is None:
            continue
        attr = _find_app_attr(module, app)
        if attr:
            return f"{module.__name__}:{attr}"
    for module in list(sys.modules.values()):
        if module is None:
            continue
        attr = _find_app_attr(module, app)
        if attr:
            return f"{module.__name__}:{attr}"
    return None


def _load_module(name: str) -> ModuleType | None:
    try:
        return importlib.import_module(name)
    except (ImportError, AttributeError, ModuleNotFoundError, TypeError, ValueError):  # pragma: no cover - best effort
        return None


def _find_app_attr(module: ModuleType, app: Celery) -> str | None:
    try:
        items = vars(module).items()
    except (AttributeError, TypeError):  # pragma: no cover - best effort
        return None
    for attr, value in items:
        if value is app:
            return str(attr)
    return None


__all__ = [
    "BeatConfig",
    "CeleryCnC",
    "CeleryCnCConfig",
    "DatabaseConfigSqlite",
    "FrontendConfig",
    "LoggingConfigFile",
    "McpConfig",
    "OpenTelemetryConfig",
    "PrometheusConfig",
]
