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

from celery_root.config import CeleryRootConfig, McpConfig, get_settings
from celery_root.core.db.rpc_client import DbRpcClient
from celery_root.optional import require_optional_scope

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from fastmcp import FastMCP
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier
    from starlette.requests import Request
    from starlette.responses import Response

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
    require_optional_scope("mcp")
    if os.environ.get("DJANGO_SETTINGS_MODULE"):
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    import django  # noqa: PLC0415

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


def _build_mcp_url(config: McpConfig) -> str:
    host = config.host
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        host = "127.0.0.1"
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    path = _normalize_path(config.path)
    if path != "/" and not path.endswith("/"):
        path = f"{path}/"
    return f"http://{host}:{config.port}{path}"


def _build_auth(config: McpConfig) -> StaticTokenVerifier | None:
    if not config.auth_key:
        return None
    from fastmcp.server.auth.providers.jwt import StaticTokenVerifier  # noqa: PLC0415

    return StaticTokenVerifier(tokens={config.auth_key: {"client_id": "celery_root_mcp"}})


def _require_auth(config: McpConfig) -> StaticTokenVerifier:
    auth = _build_auth(config)
    if auth is None:
        msg = "CELERY_ROOT_MCP_AUTH_KEY must be set when MCP is enabled."
        raise RuntimeError(msg)
    return auth


def _require_mcp_config(config: CeleryRootConfig) -> McpConfig:
    mcp_config = config.mcp
    if mcp_config is None:
        msg = "MCP is disabled in the current configuration."
        raise RuntimeError(msg)
    return mcp_config


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


_DB_TABLE_CATALOG: tuple[tuple[str, str], ...] = (
    ("tasks", "Latest task state (one row per task_id)."),
    ("task_events", "Raw task event stream (state transitions and timings)."),
    ("task_relations", "Edges between tasks in a workflow graph."),
    ("workers", "Latest worker status, heartbeat, queues, and registered tasks."),
    ("worker_events", "Raw worker event stream (online/offline/heartbeat)."),
    ("broker_queue_events", "Queue depth snapshots per broker/queue."),
    ("schedules", "Beat schedules stored in the DB."),
    ("schema_version", "Database schema version tracker."),
)

_DB_QUERY_EXAMPLES: tuple[dict[str, object], ...] = (
    {
        "description": "Recent tasks (latest 50).",
        "sql": (
            "SELECT task_id, name, state, worker, started, finished, runtime "
            "FROM tasks ORDER BY finished DESC LIMIT 50;"
        ),
    },
    {
        "description": "Latest task events (latest 50).",
        "sql": ("SELECT task_id, name, state, worker, timestamp FROM task_events ORDER BY timestamp DESC LIMIT 50;"),
    },
    {
        "description": "Workers overview.",
        "sql": ("SELECT hostname, status, last_heartbeat, active_tasks, pool_size FROM workers ORDER BY hostname;"),
    },
    {
        "description": "Queue depth snapshots (latest 50).",
        "sql": (
            "SELECT broker_url, queue, messages, consumers, timestamp "
            "FROM broker_queue_events ORDER BY timestamp DESC LIMIT 50;"
        ),
    },
    {
        "description": "Beat schedules.",
        "sql": (
            "SELECT schedule_id, name, task, schedule, enabled, last_run_at, total_run_count "
            "FROM schedules ORDER BY name;"
        ),
    },
    {
        "description": "Task relations for a workflow root id.",
        "sql": ("SELECT root_id, parent_id, child_id, relation FROM task_relations WHERE root_id = :root_id;"),
        "params": {"root_id": "<root-id>"},
    },
)


def _db_catalog_payload() -> dict[str, object]:
    return {
        "tables": [{"name": name, "description": description} for name, description in _DB_TABLE_CATALOG],
        "examples": list(_DB_QUERY_EXAMPLES),
        "notes": [
            "Use fetch_schema for column-level details.",
            "db_query supports named parameters (e.g. :root_id) via the params argument.",
            "Use db_query for read-only SQL access.",
        ],
    }


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


def _register_mcp_health(
    mcp: FastMCP,
    auth: StaticTokenVerifier,
    base_path: str,
    mcp_url: str,
) -> None:
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser  # noqa: PLC0415
    from starlette.responses import JSONResponse  # noqa: PLC0415

    health_path = "/health" if base_path == "/" else f"{base_path}/health"
    health_url = f"{mcp_url}health"

    def _health_payload() -> dict[str, object]:
        return {
            "status": "ok",
            "mcp_url": mcp_url,
            "health_url": health_url,
            "path": base_path,
        }

    @mcp.custom_route(health_path, methods=["GET"])
    async def health_check(_request: Request) -> Response:
        auth_user = _request.scope.get("user")
        if not isinstance(auth_user, AuthenticatedUser):
            auth_error_description = (
                "Authentication failed. The provided bearer token is invalid, expired, or "
                "no longer recognized by the server. To resolve: clear authentication "
                "tokens in your MCP client and reconnect. Your client should automatically "
                "re-register and obtain new tokens."
            )
            return JSONResponse(
                {
                    "error": "invalid_token",
                    "error_description": auth_error_description,
                },
                status_code=401,
                headers={
                    "WWW-Authenticate": (f'Bearer error="invalid_token", error_description="{auth_error_description}"'),
                },
            )
        auth_credentials = _request.scope.get("auth")
        scopes = set(getattr(auth_credentials, "scopes", []))
        for required_scope in auth.required_scopes:
            if required_scope not in scopes:
                scope_error_description = f"Required scope: {required_scope}"
                return JSONResponse(
                    {
                        "error": "insufficient_scope",
                        "error_description": scope_error_description,
                    },
                    status_code=403,
                    headers={
                        "WWW-Authenticate": (
                            f'Bearer error="insufficient_scope", error_description="{scope_error_description}"'
                        ),
                    },
                )
        return JSONResponse(_health_payload())

    @mcp.resource(
        "resource://celery-root/health",
        name="celery_root_health",
        description="Basic health metadata for the Celery Root MCP server.",
        mime_type="application/json",
    )
    def mcp_health_resource() -> dict[str, object]:
        return _health_payload()


def _register_mcp_db_catalog_resource(mcp: FastMCP) -> None:
    @mcp.resource(
        "resource://celery-root/db-catalog",
        name="celery_root_db_catalog",
        description=("Overview of database tables exposed via db_query (tasks, events, workers, schedules, brokers)."),
        mime_type="application/json",
    )
    def mcp_db_catalog_resource() -> dict[str, object]:
        return _db_catalog_payload()


def _register_mcp_tools(mcp: FastMCP, config: CeleryRootConfig) -> None:
    @mcp.tool(name="fetch_schema")
    def fetch_schema() -> dict[str, object]:
        """Return the current database schema as structured metadata."""
        with DbRpcClient.from_config(config, client_name="mcp") as db:
            schema = db.get_schema()
        return cast("dict[str, object]", schema.model_dump(mode="json"))

    @mcp.tool(name="db_info")
    def db_info() -> dict[str, object]:
        """Return database backend metadata."""
        with DbRpcClient.from_config(config, client_name="mcp") as db:
            info = db.get_db_info()
        return cast("dict[str, object]", info.model_dump(mode="json"))

    @mcp.tool(name="db_query")
    def db_query(
        query: str,
        params: dict[str, object] | None = None,
        max_rows: int | None = None,
    ) -> dict[str, object]:
        """Execute a read-only SQL query against Celery Root tables.

        Common tables include: tasks, task_events, task_relations, workers,
        worker_events, broker_queue_events, schedules, schema_version.
        """
        with DbRpcClient.from_config(config, client_name="mcp") as db:
            result = db.raw_query(query, params=params, max_rows=max_rows)
        return cast("dict[str, object]", result.model_dump(mode="json"))

    @mcp.tool(name="stats")
    def stats() -> dict[str, object]:
        """Return dashboard statistics equivalent to the UI frontend."""
        _ensure_django()
        from celery_root.components.web.views import dashboard as dashboard_views  # noqa: PLC0415

        payload = dashboard_views.dashboard_stats()
        with DbRpcClient.from_config(config, client_name="mcp") as db:
            task_stats = _fetch_task_stats(db)
        return cast("dict[str, object]", {**payload, "task_stats": task_stats})


def create_mcp_server() -> FastMCP:
    """Create the FastMCP server with Celery Root tools."""
    config = get_settings()
    mcp_config = _require_mcp_config(config)
    require_optional_scope("mcp")
    from fastmcp import FastMCP  # noqa: PLC0415

    auth = _require_auth(mcp_config)
    mcp = FastMCP(name="Celery Root MCP", auth=auth)
    base_path = _normalize_path(mcp_config.path)
    mcp_url = _build_mcp_url(mcp_config)
    _register_mcp_health(mcp, auth, base_path, mcp_url)
    _register_mcp_db_catalog_resource(mcp)
    _register_mcp_tools(mcp, config)

    return mcp


def create_asgi_app() -> ASGIApp:
    """Create the ASGI app for the MCP server."""
    config = get_settings()
    mcp_config = _require_mcp_config(config)
    require_optional_scope("mcp")
    server = create_mcp_server()
    base_path = _normalize_path(mcp_config.path)
    base_path_slash = f"{base_path}/" if base_path != "/" else "/"
    app: ASGIApp = cast("ASGIApp", server.http_app(path=base_path_slash))
    app = _normalize_mcp_path(app, base_path)
    return _inject_bearer_from_query(app)
