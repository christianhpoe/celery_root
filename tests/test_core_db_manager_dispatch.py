# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, LoggingConfigFile
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.dispatch import _db_info, _raw_query, _store_relations
from celery_root.core.db.manager import DBManager
from celery_root.shared.schemas import DbInfoRequest, RawQueryRequest, RpcRequestEnvelope
from celery_root.shared.schemas.domain import TaskEvent

if TYPE_CHECKING:
    from pathlib import Path


def test_dispatch_helpers(tmp_path: Path) -> None:
    db_path = tmp_path / "root.db"
    controller = SQLiteController(db_path)
    controller.initialize()
    controller.ensure_schema()

    event = TaskEvent(task_id="t1", name="demo", state="SUCCESS", timestamp=datetime.now(UTC))
    _store_relations(controller, event)

    info = _db_info(controller, DbInfoRequest())
    assert info.backend == "sqlite"

    result = _raw_query(
        controller,
        request=RawQueryRequest(query="select 1 as value", params=None, max_rows=10),
    )
    assert result.rows

    controller.close()


def test_db_manager_dispatch_errors(tmp_path: Path) -> None:
    db_path = tmp_path / "root.db"
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=db_path, rpc_max_message_bytes=10),
    )
    manager = DBManager(config)

    # Oversized message
    lock = threading.Lock()
    response = manager._dispatch(b"x" * 100, controller=SQLiteController(db_path), lock=lock)
    assert b"MESSAGE_TOO_LARGE" in response

    # Unknown op
    manager._config.database.rpc_max_message_bytes = 1024
    envelope = RpcRequestEnvelope(request_id="req", op="unknown")
    response = manager._dispatch(
        envelope.model_dump_json().encode("utf-8"),
        controller=SQLiteController(db_path),
        lock=lock,
    )
    assert b"OP_NOT_FOUND" in response
