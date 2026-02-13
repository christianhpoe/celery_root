# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Helpers for MCP database access."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import create_engine

from celery_root.config import get_settings

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

    For non-SQLite databases, set CELERY_ROOT_MCP_READONLY_DB_URL to a
    read-only account. For SQLite, this opens the file in read-only mode
    to let the database enforce write rejection.
    """
    config = get_settings()
    mcp_config = config.mcp
    if mcp_config is None:
        msg = "MCP is disabled in the current configuration."
        raise RuntimeError(msg)
    url = mcp_config.readonly_db_url
    if url:
        return create_engine(url, future=True)

    path = Path(config.database.db_path).expanduser().resolve()
    if not path.exists():
        msg = f"SQLite database file not found: {path}"
        raise RuntimeError(msg)
    # Use SQLite read-only URI so writes fail at the DB layer.
    sqlite_url = f"sqlite:///{path.as_posix()}?mode=ro"
    return create_engine(sqlite_url, future=True, connect_args={"uri": True})
