"""Settings page views."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from django.conf import settings
from django.shortcuts import render

from celery_cnc.components.web.components import component_snapshot

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
    host = getattr(settings, "CELERY_CNC_MCP_HOST", "127.0.0.1")
    port = getattr(settings, "CELERY_CNC_MCP_PORT", 9100)
    path = getattr(settings, "CELERY_CNC_MCP_PATH", "/mcp/")
    if not str(path).startswith("/"):
        path = f"/{path}"
    path = f"{str(path).rstrip('/')}/"
    mcp_url = f"http://{host}:{port}{path}"
    mcp_config = {
        "mcpServers": {
            "celery_cnc": {
                "url": mcp_url,
                "headers": {"Authorization": "Bearer <YOUR_MCP_AUTH_KEY>"},
            },
        },
    }
    claude_mcp_config = {
        "mcpServers": {
            "celery_cnc": {
                "type": "http",
                "url": mcp_url,
                "headers": {"Authorization": "Bearer <YOUR_MCP_AUTH_KEY>"},
            },
        },
    }
    codex_command = f'codex mcp add celery_cnc --url "{mcp_url}?token=<YOUR_MCP_AUTH_KEY>"'
    claude_command = (
        f'claude mcp add --transport http celery_cnc {mcp_url} --header "Authorization: Bearer <YOUR_MCP_AUTH_KEY>"'
    )
    return render(
        request,
        "settings.html",
        {
            "title": "Settings",
            "themes": _THEMES,
            "prometheus_component": prometheus_component,
            "otel_component": otel_component,
            "mcp_enabled": mcp_component.enabled,
            "mcp_auth_configured": getattr(settings, "CELERY_CNC_MCP_AUTH_KEY_SET", False),
            "mcp_url": mcp_url,
            "mcp_config_snippet": json.dumps(mcp_config, indent=2),
            "mcp_codex_command": codex_command,
            "mcp_claude_config_snippet": json.dumps(claude_mcp_config, indent=2),
            "mcp_claude_command": claude_command,
        },
    )
