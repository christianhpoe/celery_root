"""Configuration settings for Celery CnC."""

from __future__ import annotations

import secrets
from dataclasses import dataclass
from pathlib import Path

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

MAX_PORT = 65_535


class CeleryCnCConfig(BaseSettings):
    """Central configuration for Celery CnC."""

    model_config = SettingsConfigDict(
        env_prefix="CELERY_CNC_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        validate_assignment=True,
        extra="ignore",
    )

    log_dir: Path = Path("logs")
    log_rotation_hours: int = Field(default=24, gt=0)
    log_level: str = "INFO"

    event_queue_maxsize: int = Field(default=10_000, gt=0)

    schedule_path: Path | None = None
    delete_schedules_on_boot: bool = False

    prometheus: bool = Field(default=False)
    prometheus_port: int = Field(default=8001, ge=1, le=MAX_PORT)
    prometheus_path: str = Field(default="/metrics")

    opentelemetry: bool = Field(default=False)
    otel_endpoint: str = "http://localhost:4317"

    db_path: Path = Path("celery_cnc.db")
    retention_days: int = Field(default=7, gt=0)
    batch_size: int = Field(default=500, gt=0)
    flush_interval: float = Field(default=1.0, gt=0)
    purge_db: bool = Field(default=False)

    web_host: str = "127.0.0.1"
    web_port: int = Field(default=8000, ge=1, le=MAX_PORT)
    web_debug: bool = True
    web_enabled: bool = True
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

    integration: bool = False

    mcp_enabled: bool = False
    mcp_auth_key: str | None = None
    mcp_host: str = "127.0.0.1"
    mcp_port: int = Field(default=9100, ge=1, le=MAX_PORT)
    mcp_path: str = "/mcp/"
    mcp_readonly_db_url: str | None = None

    @field_validator("db_path", "log_dir", "schedule_path", mode="after")
    @classmethod
    def _expand_paths(cls, value: Path | None) -> Path | None:
        if value is None:
            return None
        return Path(value).expanduser()


@dataclass
class _SettingsState:
    cache: CeleryCnCConfig | None = None
    runtime: CeleryCnCConfig | None = None


_STATE = _SettingsState()


def get_settings() -> CeleryCnCConfig:
    """Return the active configuration, reading from the environment if needed."""
    if _STATE.runtime is not None:
        return _STATE.runtime
    if _STATE.cache is None:
        _STATE.cache = CeleryCnCConfig()
    return _STATE.cache


def set_settings(config: CeleryCnCConfig) -> None:
    """Override the global settings for the current process."""
    _STATE.runtime = config


def reset_settings() -> None:
    """Clear cached settings so environment changes are re-read."""
    _STATE.cache = None
    _STATE.runtime = None


__all__ = [
    "MAX_PORT",
    "CeleryCnCConfig",
    "get_settings",
    "reset_settings",
    "set_settings",
]
