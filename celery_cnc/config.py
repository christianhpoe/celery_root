"""Configuration settings for Celery CnC."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from pathlib import Path
from shutil import rmtree

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

MAX_PORT = 65_535


class LoggingConfigFile(BaseModel):
    """File-based logging configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    log_dir: Path = Path("logs")
    log_rotation_hours: int = Field(default=24, gt=0)
    log_level: str = "INFO"
    delete_on_boot: bool = False

    @field_validator("log_dir", mode="after")
    @classmethod
    def _expand_log_dir(cls, value: Path) -> Path:
        return value.expanduser()

    @model_validator(mode="after")
    def _ensure_log_dir(self) -> LoggingConfigFile:
        if self.delete_on_boot and self.log_dir.exists():
            for entry in self.log_dir.iterdir():
                if entry.is_dir():
                    rmtree(entry)
                else:
                    entry.unlink(missing_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        return self


class DatabaseConfigSqlite(BaseModel):
    """SQLite database configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    db_path: Path = Path("celery_cnc.db")
    retention_days: int = Field(default=7, gt=0)
    batch_size: int = Field(default=500, gt=0)
    flush_interval: float = Field(default=1.0, gt=0)
    purge_db: bool = False

    @field_validator("db_path", mode="after")
    @classmethod
    def _expand_db_path(cls, value: Path) -> Path:
        return value.expanduser()

    @model_validator(mode="after")
    def _ensure_db_parent(self) -> DatabaseConfigSqlite:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        return self


class BeatConfig(BaseModel):
    """Beat scheduler configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    schedule_path: Path | None = None
    delete_schedules_on_boot: bool = False

    @field_validator("schedule_path", mode="after")
    @classmethod
    def _expand_schedule_path(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        return value.expanduser()

    @model_validator(mode="after")
    def _ensure_schedule_parent(self) -> BeatConfig:
        if self.schedule_path is not None:
            self.schedule_path.parent.mkdir(parents=True, exist_ok=True)
        return self


class PrometheusConfig(BaseModel):
    """Prometheus exporter configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    port: int = Field(default=8001, ge=1, le=MAX_PORT)
    prometheus_path: str = "/metrics"
    flower_comatibility: bool = False

    @field_validator("prometheus_path", mode="after")
    @classmethod
    def _normalize_path(cls, value: str) -> str:
        if not value:
            return "/metrics"
        return value if value.startswith("/") else f"/{value}"

    @property
    def flower_compatibility(self) -> bool:
        """Return whether to use Flower metric naming."""
        return self.flower_comatibility


class OpenTelemetryConfig(BaseModel):
    """OpenTelemetry exporter configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    endpoint: str = "http://localhost:4317"
    service_name: str = "celery-cnc"


class FrontendConfig(BaseModel):
    """Web frontend configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    host: str = "127.0.0.1"
    port: int = Field(default=8000, ge=1, le=MAX_PORT)
    debug: bool = True
    poll_interval: float = Field(default=2.0, gt=0)
    secret_key: str = Field(default_factory=lambda: secrets.token_urlsafe(32))

    basic_auth: str | None = None
    auth_provider: str | None = None
    auth: str | None = None
    oauth2_key: str | None = None
    oauth2_secret: str | None = None
    oauth2_redirect_uri: str | None = None
    oauth2_okta_base_url: str | None = None
    gitlab_allowed_groups: str | None = None
    gitlab_min_access_level: int | None = Field(default=None, ge=1)
    gitlab_oauth_domain: str | None = None


class McpConfig(BaseModel):
    """MCP server configuration."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    host: str = "127.0.0.1"
    port: int = Field(default=9100, ge=1, le=MAX_PORT)
    path: str = "/mcp/"
    auth_key: str | None = None
    readonly_db_url: str | None = None

    @field_validator("path", mode="after")
    @classmethod
    def _normalize_path(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            return "/mcp/"
        if not cleaned.startswith("/"):
            cleaned = f"/{cleaned}"
        return cleaned


class CeleryCnCConfig(BaseModel):
    """Central configuration for Celery CnC."""

    model_config = ConfigDict(validate_assignment=True, extra="ignore")

    logging: LoggingConfigFile = Field(default_factory=LoggingConfigFile)
    database: DatabaseConfigSqlite = Field(default_factory=DatabaseConfigSqlite)
    beat: BeatConfig | None = None
    prometheus: PrometheusConfig | None = None
    open_telemetry: OpenTelemetryConfig | None = None
    frontend: FrontendConfig | None = Field(default_factory=FrontendConfig)
    mcp: McpConfig | None = None

    event_queue_maxsize: int = Field(default=100_000, gt=0)
    integration: bool = False


@dataclass
class _SettingsState:
    cache: CeleryCnCConfig | None = None
    runtime: CeleryCnCConfig | None = None


_STATE = _SettingsState()


def get_settings() -> CeleryCnCConfig:
    """Return the active configuration, reading defaults if needed."""
    if _STATE.runtime is not None:
        return _STATE.runtime
    if _STATE.cache is None:
        _STATE.cache = CeleryCnCConfig()
    return _STATE.cache


def set_settings(config: CeleryCnCConfig) -> None:
    """Override the global settings for the current process."""
    _STATE.runtime = config


def reset_settings() -> None:
    """Clear cached settings."""
    _STATE.cache = None
    _STATE.runtime = None


__all__ = [
    "MAX_PORT",
    "BeatConfig",
    "CeleryCnCConfig",
    "DatabaseConfigSqlite",
    "FrontendConfig",
    "LoggingConfigFile",
    "McpConfig",
    "OpenTelemetryConfig",
    "PrometheusConfig",
    "get_settings",
    "reset_settings",
    "set_settings",
]
