# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import json
import os
import secrets
from typing import TYPE_CHECKING

import django
import pytest
from django.http import HttpResponse
from django.test import RequestFactory

from celery_root.components.web.components import ComponentInfo
from celery_root.components.web.views import settings as settings_views
from celery_root.config import (
    CeleryRootConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    LoggingConfigFile,
    McpConfig,
    PrometheusConfig,
)

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


def _fake_render(_request: object, _template: str, context: dict[str, object]) -> HttpResponse:
    payload = {
        "prometheus_scrape_snippet": context.get("prometheus_scrape_snippet"),
        "mcp_url": context.get("mcp_url"),
        "mcp_config_snippet": context.get("mcp_config_snippet"),
        "mcp_codex_command": context.get("mcp_codex_command"),
        "mcp_claude_config_snippet": context.get("mcp_claude_config_snippet"),
    }
    return HttpResponse(json.dumps(payload), content_type="application/json")


def test_settings_page_builds_snippets(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    frontend_host = "0.0.0.0"  # noqa: S104
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite"),
        prometheus=PrometheusConfig(port=9001, prometheus_path="metrics"),
        frontend=FrontendConfig(
            host=frontend_host,
            port=7000,
            debug=False,
            secret_key=secrets.token_urlsafe(16),
        ),
        mcp=McpConfig(host="127.0.0.1", port=9100, path="mcp", auth_key="token"),
    )
    snapshot = {
        "prometheus": ComponentInfo(
            key="prometheus",
            display_name="Prometheus",
            enabled=True,
            status="up",
            pid=111,
            url="http://127.0.0.1:9001/metrics",
        ),
        "open_telemetry": ComponentInfo(
            key="open_telemetry",
            display_name="OpenTelemetry",
            enabled=False,
            status="down",
            pid=None,
        ),
        "mcp": ComponentInfo(
            key="mcp",
            display_name="MCP",
            enabled=True,
            status="up",
            pid=222,
            url="http://127.0.0.1:9100/mcp/",
        ),
    }
    monkeypatch.setattr(settings_views, "get_settings", lambda: config)
    monkeypatch.setattr(settings_views, "component_snapshot", lambda: snapshot)
    monkeypatch.setattr(settings_views, "render", _fake_render)

    request = RequestFactory().get("/settings/")
    response = settings_views.settings_page(request)
    assert response.status_code == 200

    payload = json.loads(response.content)
    scrape_snippet = payload["prometheus_scrape_snippet"]
    assert "metrics_path: /metrics" in scrape_snippet
    assert "targets: ['127.0.0.1:9001']" in scrape_snippet

    assert payload["mcp_url"] == "http://127.0.0.1:9100/mcp/"

    mcp_config = json.loads(payload["mcp_config_snippet"])
    assert mcp_config["mcpServers"]["celery_root"]["url"] == "http://127.0.0.1:9100/mcp/"
    assert mcp_config["mcpServers"]["celery_root"]["headers"]["Authorization"] == "Bearer token"

    assert payload["mcp_codex_command"] == 'codex mcp add celery_root --url "http://127.0.0.1:9100/mcp/?token=token"'

    claude_config = json.loads(payload["mcp_claude_config_snippet"])
    assert claude_config["mcpServers"]["celery_root"]["type"] == "http"
    assert claude_config["mcpServers"]["celery_root"]["url"] == "http://127.0.0.1:9100/mcp/"
