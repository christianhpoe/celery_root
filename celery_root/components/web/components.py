# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Component metadata helpers for the web UI."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from celery_root.config import get_settings
from celery_root.core.component_status import ComponentStatus, ComponentStatusStore, is_pid_alive

if TYPE_CHECKING:
    from collections.abc import Mapping


@dataclass(slots=True)
class ComponentInfo:
    """Summary metadata for a runtime component."""

    key: str
    display_name: str
    enabled: bool
    status: str
    pid: int | None
    url: str | None = None
    config: dict[str, object] | None = None


def component_snapshot() -> dict[str, ComponentInfo]:
    """Return component status metadata for UI rendering."""
    config = get_settings()
    status_store = ComponentStatusStore.from_config(config)
    statuses = status_store.read()

    prometheus = config.prometheus
    otel = config.open_telemetry
    beat = config.beat
    frontend = config.frontend
    mcp = config.mcp

    prometheus_status = _process_component_status(statuses, "prometheus")
    otel_status = _process_component_status(statuses, "otel")
    web_status = _process_component_status(statuses, "web")
    mcp_status = _process_component_status(statuses, "mcp")

    snapshot: dict[str, ComponentInfo] = {
        "prometheus": ComponentInfo(
            key="prometheus",
            display_name="Prometheus",
            enabled=prometheus is not None,
            status=_status_label(enabled=prometheus is not None, alive=prometheus_status.alive),
            pid=prometheus_status.pid,
            url=_metrics_url() if prometheus is not None else None,
            config=({"port": prometheus.port, "path": prometheus.prometheus_path} if prometheus is not None else None),
        ),
        "open_telemetry": ComponentInfo(
            key="open_telemetry",
            display_name="OpenTelemetry",
            enabled=otel is not None,
            status=_status_label(enabled=otel is not None, alive=otel_status.alive),
            pid=otel_status.pid,
            config=({"endpoint": otel.endpoint, "service_name": otel.service_name} if otel is not None else None),
        ),
        "beat": ComponentInfo(
            key="beat",
            display_name="Beat",
            enabled=beat is not None,
            status="configured" if beat is not None else "disabled",
            pid=None,
            config=(
                {
                    "schedule_path": str(beat.schedule_path) if beat.schedule_path else None,
                    "delete_on_boot": beat.delete_schedules_on_boot,
                }
                if beat is not None
                else None
            ),
        ),
        "frontend": ComponentInfo(
            key="frontend",
            display_name="Web",
            enabled=frontend is not None,
            status=_status_label(enabled=frontend is not None, alive=web_status.alive),
            pid=web_status.pid,
            url=_frontend_url() if frontend is not None else None,
            config=({"host": frontend.host, "port": frontend.port} if frontend is not None else None),
        ),
        "mcp": ComponentInfo(
            key="mcp",
            display_name="MCP",
            enabled=mcp is not None,
            status=_status_label(enabled=mcp is not None, alive=mcp_status.alive),
            pid=mcp_status.pid,
            url=_mcp_url() if mcp is not None else None,
            config=(
                {
                    "host": mcp.host,
                    "port": mcp.port,
                    "path": mcp.path,
                    "auth_configured": bool(mcp.auth_key),
                }
                if mcp is not None
                else None
            ),
        ),
    }
    return snapshot


@dataclass(slots=True)
class _ProcessStatus:
    pid: int | None
    alive: bool


def _process_component_status(statuses: Mapping[str, ComponentStatus], name: str) -> _ProcessStatus:
    status = statuses.get(name)
    pid = getattr(status, "pid", None)
    alive = getattr(status, "alive", False)
    if pid is not None:
        alive = alive and is_pid_alive(pid)
    return _ProcessStatus(pid=pid, alive=bool(alive))


def _status_label(*, enabled: bool, alive: bool) -> str:
    if not enabled:
        return "disabled"
    return "up" if alive else "down"


def _metrics_url() -> str:
    config = get_settings()
    frontend = config.frontend
    host = frontend.host if frontend is not None else "127.0.0.1"
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    prometheus = config.prometheus
    path = "/metrics" if prometheus is None else prometheus.prometheus_path
    port = 8001 if prometheus is None else prometheus.port
    return f"http://{host}:{port}{path}"


def _frontend_url() -> str:
    config = get_settings()
    frontend = config.frontend
    if frontend is None:
        return ""
    host = frontend.host
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"http://{host}:{frontend.port}/"


def _mcp_url() -> str:
    config = get_settings()
    mcp = config.mcp
    if mcp is None:
        return ""
    host = mcp.host
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    path = mcp.path or "/mcp/"
    if not path.startswith("/"):
        path = f"/{path}"
    path = f"{path.rstrip('/')}/"
    return f"http://{host}:{mcp.port}{path}"
