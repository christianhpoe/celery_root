# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from celery_root.components.mcp import server as mcp_server
from celery_root.config import (
    CeleryRootConfig,
    DatabaseConfigSqlite,
    LoggingConfigFile,
    McpConfig,
    get_settings,
    set_settings,
)
from celery_root.core.db.models import Task

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, MutableMapping
    from pathlib import Path

    from celery_root.core.db.rpc_client import DbRpcClient


def test_normalize_path() -> None:
    assert mcp_server._normalize_path("mcp") == "/mcp"
    assert mcp_server._normalize_path("/mcp/") == "/mcp"


def test_percentile() -> None:
    values = [1.0, 2.0, 3.0]
    assert mcp_server._percentile(values, 0.0) == 1.0
    assert mcp_server._percentile(values, 1.0) == 3.0


@pytest.mark.asyncio
async def test_inject_bearer_and_normalize_path() -> None:
    received: list[MutableMapping[str, object]] = []

    async def app(
        scope: MutableMapping[str, object],
        _receive: Callable[[], Awaitable[MutableMapping[str, object]]],
        _send: Callable[[MutableMapping[str, object]], Awaitable[None]],
    ) -> None:
        received.append(scope)

    wrapped = mcp_server._inject_bearer_from_query(app)
    scope: MutableMapping[str, object] = {
        "type": "http",
        "headers": [],
        "query_string": b"token=abc",
        "path": "/mcp",
    }

    async def receive() -> MutableMapping[str, object]:
        return {}

    async def send(_message: MutableMapping[str, object]) -> None:
        return None

    await wrapped(scope, receive, send)
    headers = cast("list[tuple[bytes, bytes]]", scope.get("headers", []))
    assert any(key == b"authorization" for key, _ in headers)

    normalized = mcp_server._normalize_mcp_path(app, "/mcp")
    scope2: MutableMapping[str, object] = {"type": "http", "path": "/mcp"}
    await normalized(scope2, receive, send)
    assert scope2["path"] == "/mcp/"


def test_fetch_task_stats() -> None:
    tasks = [
        Task(task_id="t1", name="demo", state="SUCCESS", runtime=1.0),
        Task(task_id="t2", name="demo", state="FAILURE", runtime=2.0),
    ]

    class _DummyDb:
        def get_tasks(self) -> list[Task]:
            return tasks

    rows = mcp_server._fetch_task_stats(cast("DbRpcClient", _DummyDb()))
    assert rows[0]["count"] == 2


def test_create_server_errors(tmp_path: Path) -> None:
    original = get_settings()
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "root.db"),
        mcp=None,
    )
    try:
        set_settings(config)
        with pytest.raises(RuntimeError):
            mcp_server.create_mcp_server()

        config = CeleryRootConfig(
            logging=LoggingConfigFile(log_dir=tmp_path / "logs2"),
            database=DatabaseConfigSqlite(db_path=tmp_path / "root2.db"),
            mcp=McpConfig(auth_key=None),
        )
        set_settings(config)
        with pytest.raises(RuntimeError):
            mcp_server.create_mcp_server()
    finally:
        set_settings(original)


def test_create_asgi_app(tmp_path: Path) -> None:
    original = get_settings()
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs3"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db3.sqlite"),
        mcp=McpConfig(auth_key="token"),
    )
    try:
        set_settings(config)
        app = mcp_server.create_asgi_app()
        assert callable(app)
    finally:
        set_settings(original)
