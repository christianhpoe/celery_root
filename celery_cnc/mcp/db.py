"""Helpers for MCP database access."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from celery_cnc.config import get_settings

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


@dataclass(slots=True)
class QueryResult:
    """Serializable result of a SQL query."""

    columns: list[str]
    rows: list[dict[str, object]]
    truncated: bool


def create_readonly_engine() -> Engine:
    """Create a read-only SQLAlchemy engine for MCP queries.

    For non-SQLite databases, set CELERY_CNC_MCP_READONLY_DB_URL to a
    read-only account. For SQLite, this opens the file in read-only mode
    to let the database enforce write rejection.
    """
    config = get_settings()
    url = config.mcp_readonly_db_url
    if url:
        return create_engine(url, future=True)

    path = Path(config.db_path).expanduser().resolve()
    if not path.exists():
        msg = f"SQLite database file not found: {path}"
        raise RuntimeError(msg)
    # Use SQLite read-only URI so writes fail at the DB layer.
    sqlite_url = f"sqlite:///{path.as_posix()}?mode=ro"
    return create_engine(sqlite_url, future=True, connect_args={"uri": True})
