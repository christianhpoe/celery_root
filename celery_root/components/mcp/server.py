# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""FastMCP server for Celery Root."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

import django
from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier

from celery_root.components.web.views import dashboard as dashboard_views
from celery_root.config import McpConfig, get_settings
from celery_root.core.db.rpc_client import DbRpcClient

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from celery_root.shared.schemas.domain import Task

    ASGIMessage = MutableMapping[str, object]
    ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
    ASGISend = Callable[[ASGIMessage], Awaitable[None]]
    ASGIScope = MutableMapping[str, object]
    ASGIApp = Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]]
else:
    ASGIMessage = dict
    ASGIReceive = Callable
    ASGISend = Callable
    ASGIScope = dict
    ASGIApp = Callable


def _normalize_path(path: str) -> str:
    cleaned = path.strip()
    if not cleaned.startswith("/"):
        cleaned = f"/{cleaned}"
    return cleaned.rstrip("/") or "/"


def _ensure_django() -> None:
    if os.environ.get("DJANGO_SETTINGS_MODULE"):
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")

    django.setup()


def _inject_bearer_from_query(app: ASGIApp) -> ASGIApp:
    async def _wrapped(scope: ASGIScope, receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") == "http":
            headers = list(cast("list[tuple[bytes, bytes]]", scope.get("headers", [])))
            has_auth = any(key.lower() == b"authorization" for key, _ in headers)
            if not has_auth:
                query_bytes = cast("bytes", scope.get("query_string", b""))
                query = query_bytes.decode("ascii", errors="ignore")
                token = None
                for part in query.split("&"):
                    if part.startswith("token="):
                        token = part.split("=", 1)[1]
                        break
                if token:
                    headers.append((b"authorization", f"Bearer {token}".encode()))
                    scope["headers"] = headers
        await app(scope, receive, send)

    return _wrapped


def _normalize_mcp_path(app: ASGIApp, base_path: str) -> ASGIApp:
    base_path = base_path.rstrip("/") or "/"
    with_slash = f"{base_path}/" if base_path != "/" else "/"

    async def _wrapped(scope: ASGIScope, receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") == "http":
            path = cast("str", scope.get("path", ""))
            if path in (base_path, with_slash):
                scope["path"] = with_slash
        await app(scope, receive, send)

    return _wrapped


def _build_auth(config: McpConfig) -> StaticTokenVerifier | None:
    if not config.auth_key:
        return None
    return StaticTokenVerifier(tokens={config.auth_key: {"client_id": "celery_root_mcp"}})


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if pct <= 0:
        return values[0]
    if pct >= 1:
        return values[-1]
    index = (len(values) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    weight = index - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def _fetch_task_stats(db: DbRpcClient) -> list[dict[str, object]]:
    tasks = db.get_tasks()
    grouped: dict[str, list[Task]] = {}
    for task in tasks:
        name = task.name or "unknown"
        grouped.setdefault(name, []).append(task)
    rows: list[dict[str, object]] = []
    for name, group in grouped.items():
        runtimes = sorted([task.runtime for task in group if task.runtime is not None])
        count = len(group)
        if runtimes:
            avg = sum(runtimes) / len(runtimes)
            min_runtime = runtimes[0]
            max_runtime = runtimes[-1]
            p95 = _percentile(runtimes, 0.95)
            p99 = _percentile(runtimes, 0.99)
        else:
            avg = None
            min_runtime = None
            max_runtime = None
            p95 = None
            p99 = None
        rows.append(
            {
                "name": name,
                "count": count,
                "avg": avg,
                "p95": p95,
                "p99": p99,
                "min": min_runtime,
                "max": max_runtime,
            },
        )
    rows.sort(key=lambda row: (-int(cast("int", row["count"])), str(row["name"])))
    return rows


def create_mcp_server() -> FastMCP:
    """Create the FastMCP server with Celery Root tools."""
    config = get_settings()
    mcp_config = config.mcp
    if mcp_config is None:
        msg = "MCP is disabled in the current configuration."
        raise RuntimeError(msg)
    if not mcp_config.auth_key:
        msg = "CELERY_ROOT_MCP_AUTH_KEY must be set when MCP is enabled."
        raise RuntimeError(msg)
    mcp = FastMCP(name="Celery Root MCP", auth=_build_auth(mcp_config))

    @mcp.tool(name="fetch_schema")
    def fetch_schema() -> dict[str, object]:
        """Return the current database schema as structured metadata."""
        with DbRpcClient.from_config(config, client_name="mcp") as db:
            schema = db.get_schema()
        return cast("dict[str, object]", schema.model_dump(mode="json"))

    @mcp.tool(name="stats")
    def stats() -> dict[str, object]:
        """Return dashboard statistics equivalent to the UI frontend."""
        _ensure_django()
        payload = dashboard_views.dashboard_stats()
        with DbRpcClient.from_config(config, client_name="mcp") as db:
            task_stats = _fetch_task_stats(db)
        return cast("dict[str, object]", {**payload, "task_stats": task_stats})

    return mcp


def create_asgi_app() -> ASGIApp:
    """Create the ASGI app for the MCP server."""
    config = get_settings()
    mcp_config = config.mcp
    if mcp_config is None:
        msg = "MCP is disabled in the current configuration."
        raise RuntimeError(msg)
    server = create_mcp_server()
    base_path = _normalize_path(mcp_config.path)
    base_path_slash = f"{base_path}/" if base_path != "/" else "/"
    app: ASGIApp = cast("ASGIApp", server.http_app(path=base_path_slash))
    app = _normalize_mcp_path(app, base_path)
    return _inject_bearer_from_query(app)
