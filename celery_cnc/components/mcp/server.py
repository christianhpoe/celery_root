"""FastMCP server for Celery CnC."""

from __future__ import annotations

import os
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, cast

import django
from fastmcp import FastMCP
from fastmcp.server.auth.providers.jwt import StaticTokenVerifier
from sqlalchemy import inspect, text

from celery_cnc.components.web.views import dashboard as dashboard_views
from celery_cnc.config import McpConfig, get_settings

from .db import QueryResult, create_readonly_engine

if TYPE_CHECKING:
    from collections.abc import MutableMapping

    from sqlalchemy.engine import Row

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
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_cnc.components.web.settings")

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
    return StaticTokenVerifier(tokens={config.auth_key: {"client_id": "celery_cnc_mcp"}})


def _row_to_dict(row: Row[tuple[object, ...]]) -> dict[str, object]:
    mapping = row._mapping  # noqa: SLF001
    return {key: mapping[key] for key in mapping}


def _serialize_query_result(result: QueryResult) -> dict[str, object]:
    return {
        "columns": result.columns,
        "rows": result.rows,
        "truncated": result.truncated,
    }


def _execute_readonly_query(
    sql: str,
    params: Mapping[str, object] | None,
    limit: int,
) -> QueryResult:
    engine = create_readonly_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql), params or {})
        rows = result.fetchmany(limit + 1)
        columns = list(result.keys())
    truncated = len(rows) > limit
    if truncated:
        rows = rows[:limit]
    row_dicts = [_row_to_dict(row) for row in rows]
    return QueryResult(columns=columns, rows=row_dicts, truncated=truncated)


def _fetch_task_stats() -> list[dict[str, object]]:
    sql = """
    WITH base AS (
        SELECT COALESCE(name, 'unknown') AS name, runtime
        FROM tasks
    ),
    counts AS (
        SELECT name, COUNT(*) AS count
        FROM base
        GROUP BY name
    ),
    runtimes AS (
        SELECT name, runtime
        FROM base
        WHERE runtime IS NOT NULL
    ),
    ordered AS (
        SELECT
            name,
            runtime,
            COUNT(*) OVER (PARTITION BY name) AS n,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY runtime) AS rn
        FROM runtimes
    ),
    pcts AS (
        SELECT
            name,
            AVG(runtime) AS avg_runtime,
            MIN(runtime) AS min_runtime,
            MAX(runtime) AS max_runtime,
            MAX(CASE WHEN rn = CAST(((n - 1) * 0.95) AS INTEGER) + 1 THEN runtime END) AS p95,
            MAX(CASE WHEN rn = CAST(((n - 1) * 0.99) AS INTEGER) + 1 THEN runtime END) AS p99
        FROM ordered
        GROUP BY name
    )
    SELECT
        counts.name AS name,
        counts.count AS count,
        pcts.avg_runtime AS avg,
        pcts.p95 AS p95,
        pcts.p99 AS p99,
        pcts.min_runtime AS min,
        pcts.max_runtime AS max
    FROM counts
    LEFT JOIN pcts ON counts.name = pcts.name
    ORDER BY counts.count DESC, counts.name ASC
    """
    engine = create_readonly_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    return [_row_to_dict(row) for row in rows]


def create_mcp_server() -> FastMCP:
    """Create the FastMCP server with Celery CnC tools."""
    config = get_settings()
    mcp_config = config.mcp
    if mcp_config is None:
        msg = "MCP is disabled in the current configuration."
        raise RuntimeError(msg)
    if not mcp_config.auth_key:
        msg = "CELERY_CNC_MCP_AUTH_KEY must be set when MCP is enabled."
        raise RuntimeError(msg)
    mcp = FastMCP(name="Celery CnC MCP", auth=_build_auth(mcp_config))

    @mcp.tool(name="fetch_schema")
    def fetch_schema() -> dict[str, object]:
        """Return the current database schema as structured metadata.

        Uses a read-only database connection so that accidental writes are rejected
        by the database itself. For non-SQLite databases, configure a read-only
        account via CELERY_CNC_MCP_READONLY_DB_URL.

        Returns:
            A dictionary with the SQL dialect name and table/column details.
        """
        engine = create_readonly_engine()
        inspector = inspect(engine)
        tables: dict[str, object] = {}
        for table in inspector.get_table_names():
            columns = inspector.get_columns(table)
            tables[table] = {
                "columns": [
                    {
                        "name": column["name"],
                        "type": str(column["type"]),
                        "nullable": bool(column.get("nullable", True)),
                        "default": column.get("default"),
                        "primary_key": bool(column.get("primary_key", False)),
                    }
                    for column in columns
                ],
                "indexes": inspector.get_indexes(table),
            }
        return {
            "dialect": engine.dialect.name,
            "tables": tables,
        }

    @mcp.tool(name="run_query")
    def run_query(
        sql: str,
        params: Mapping[str, object] | None = None,
        limit: int = 500,
    ) -> dict[str, object]:
        """Execute a SQL query via a read-only connection and return rows.

        This tool is intended for SELECT-style analytics. It uses a read-only
        database connection; attempts to write will be rejected by the database.

        Args:
            sql: The SQL statement to execute. Use SELECT/WITH statements.
            params: Optional query parameters (bind variables).
            limit: Maximum number of rows to return. Additional rows are truncated.

        Returns:
            A dictionary with `columns`, `rows`, and `truncated` fields.
        """
        if limit <= 0:
            msg = "limit must be a positive integer"
            raise ValueError(msg)
        normalized = sql.lstrip().lower()
        if not normalized.startswith(("select", "with")):
            msg = "Only SELECT/WITH queries are allowed"
            raise ValueError(msg)
        return _serialize_query_result(_execute_readonly_query(sql, params, limit))

    @mcp.tool(name="stats")
    def stats() -> dict[str, object]:
        """Return dashboard statistics equivalent to the UI frontend.

        The payload mirrors the dashboard data shown in the web UI, including:
        summary cards, state breakdowns, throughput series, heatmap data,
        worker summaries, and recent activity.
        """
        _ensure_django()
        payload = dashboard_views.dashboard_stats()
        return cast("dict[str, object]", {**payload, "task_stats": _fetch_task_stats()})

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
