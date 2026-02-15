# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from celery_root.config import CeleryRootConfig, DatabaseConfigSqlite, LoggingConfigFile
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.dispatch import RPC_OPERATIONS
from celery_root.core.db.manager import DBManager, _authkey_from_config, _prepare_socket

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_authkey_from_config(tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db.sqlite", rpc_auth_key="secret"),
    )
    auth = _authkey_from_config(config)
    assert auth == b"secret"


def test_prepare_socket_removes_file(tmp_path: Path) -> None:
    path = tmp_path / "socket.sock"
    path.write_text("stale")
    _prepare_socket(path)
    assert not path.exists()


def test_handle_operation_ping(tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs2"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db2.sqlite"),
    )
    manager = DBManager(config)
    controller = SQLiteController(tmp_path / "db2.sqlite")
    lock = threading.Lock()
    op = RPC_OPERATIONS["db.ping"]
    payload = manager._handle_operation(op, None, controller, lock)
    assert payload is not None


def test_serve_loop(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    config = CeleryRootConfig(
        logging=LoggingConfigFile(log_dir=tmp_path / "logs3"),
        database=DatabaseConfigSqlite(db_path=tmp_path / "db3.sqlite", rpc_socket_path=tmp_path / "sock"),
    )
    manager = DBManager(config)

    class _DummyListener:
        def __init__(self) -> None:
            self._accepted = False

        def accept(self) -> object:
            if self._accepted:
                message = "done"
                raise OSError(message)
            self._accepted = True
            return object()

        def close(self) -> None:
            return None

    monkeypatch.setattr("celery_root.core.db.manager.Listener", lambda *_args, **_kwargs: _DummyListener())
    monkeypatch.setattr(manager, "_handle_connection", lambda *_args, **_kwargs: None)
    monkeypatch.setattr("celery_root.core.db.manager._prepare_socket", lambda *_args, **_kwargs: None)
    manager._serve(SQLiteController(tmp_path / "db3.sqlite"))
