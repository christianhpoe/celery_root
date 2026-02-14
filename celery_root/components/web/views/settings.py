# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Settings page views."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from django.conf import settings
from django.shortcuts import render

from celery_root.components.web.components import component_snapshot
from celery_root.config import get_settings

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse

_THEMES: tuple[tuple[str, str], ...] = (
    ("monokai", "Monokai"),
    ("darkula", "Darkula"),
    ("generic", "Generic"),
    ("dark", "Dark"),
    ("white", "White"),
    ("solaris", "Solaris"),
)


def settings_page(request: HttpRequest) -> HttpResponse:
    """Render the settings page."""
    snapshot = component_snapshot()
    prometheus_component = snapshot["prometheus"]
    otel_component = snapshot["open_telemetry"]
    mcp_component = snapshot["mcp"]
    config = get_settings()
    frontend = config.frontend
    prometheus = config.prometheus
    prometheus_host = frontend.host if frontend is not None else "127.0.0.1"
    if prometheus_host in {"0.0.0.0", "::"}:  # noqa: S104
        prometheus_host = "127.0.0.1"
    prometheus_port = prometheus.port if prometheus is not None else 8001
    prometheus_path = prometheus.prometheus_path if prometheus is not None else "/metrics"
    if not prometheus_path.startswith("/"):
        prometheus_path = f"/{prometheus_path}"
    prometheus_scrape_snippet = "\n".join(
        [
            "- job_name: celery_root",
            f"  metrics_path: {prometheus_path}",
            "  static_configs:",
            f"  - targets: ['{prometheus_host}:{prometheus_port}']",
        ],
    )
    mcp_config = config.mcp
    host = mcp_config.host if mcp_config is not None else getattr(settings, "CELERY_ROOT_MCP_HOST", "127.0.0.1")
    port = mcp_config.port if mcp_config is not None else getattr(settings, "CELERY_ROOT_MCP_PORT", 9100)
    path = mcp_config.path if mcp_config is not None else getattr(settings, "CELERY_ROOT_MCP_PATH", "/mcp/")
    if not str(path).startswith("/"):
        path = f"/{path}"
    path = f"{str(path).rstrip('/')}/"
    mcp_url = f"http://{host}:{port}{path}"
    auth_key = mcp_config.auth_key if mcp_config is not None and mcp_config.auth_key else "<YOUR_MCP_AUTH_KEY>"
    mcp_config_payload = {
        "mcpServers": {
            "celery_root": {
                "url": mcp_url,
                "headers": {"Authorization": f"Bearer {auth_key}"},
            },
        },
    }
    claude_mcp_config = {
        "mcpServers": {
            "celery_root": {
                "type": "http",
                "url": mcp_url,
                "headers": {"Authorization": f"Bearer {auth_key}"},
            },
        },
    }
    codex_command = f'codex mcp add celery_root --url "{mcp_url}?token={auth_key}"'
    claude_command = (
        f'claude mcp add --transport http celery_root {mcp_url} --header "Authorization: Bearer {auth_key}"'
    )
    return render(
        request,
        "settings.html",
        {
            "title": "Settings",
            "themes": _THEMES,
            "prometheus_component": prometheus_component,
            "prometheus_scrape_snippet": prometheus_scrape_snippet,
            "otel_component": otel_component,
            "mcp_enabled": mcp_component.enabled,
            "mcp_auth_configured": getattr(settings, "CELERY_ROOT_MCP_AUTH_KEY_SET", False),
            "mcp_url": mcp_url,
            "mcp_config_snippet": json.dumps(mcp_config_payload, indent=2),
            "mcp_codex_command": codex_command,
            "mcp_claude_config_snippet": json.dumps(claude_mcp_config, indent=2),
            "mcp_claude_command": claude_command,
        },
    )
