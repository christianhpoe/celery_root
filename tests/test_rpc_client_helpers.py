# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, cast

import pytest

from celery_root.core.db import rpc_client
from celery_root.shared.schemas import RpcResponseEnvelope

if TYPE_CHECKING:
    from multiprocessing.connection import Connection


class _DummyConnection:
    def __init__(self, response: bytes, *, poll_ok: bool = True) -> None:
        self._response = response
        self._poll_ok = poll_ok
        self.sent: bytes | None = None

    def send_bytes(self, data: bytes) -> None:
        self.sent = data

    def poll(self, _timeout: float) -> bool:
        return self._poll_ok

    def recv_bytes(self) -> bytes:
        return self._response

    def close(self) -> None:
        return None


class _DummyTransport(rpc_client._RpcTransport):
    def __init__(self, response: bytes, *, poll_ok: bool = True) -> None:
        settings = rpc_client._RpcSettings(address="addr", authkey=None, timeout_seconds=0.1, max_message_bytes=1024)
        super().__init__(settings, client_name="tests")
        self._dummy = _DummyConnection(response, poll_ok=poll_ok)

    def connect(self) -> None:
        self._connection = cast("Connection", self._dummy)


def test_rpc_transport_success(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeUuid:
        hex = "req"

    def _fake_uuid4() -> _FakeUuid:
        return _FakeUuid()

    monkeypatch.setattr(uuid, "uuid4", _fake_uuid4)
    response = RpcResponseEnvelope(request_id="req", ok=True, payload={}, timestamp=datetime.now(UTC))
    transport = _DummyTransport(response.model_dump_json().encode("utf-8"))
    result = transport.request("db.ping", {}, timeout_seconds=0.01)
    assert result.ok


def test_rpc_transport_timeout() -> None:
    response = RpcResponseEnvelope(request_id="req", ok=True, payload={}, timestamp=datetime.now(UTC))
    transport = _DummyTransport(response.model_dump_json().encode("utf-8"), poll_ok=False)
    with pytest.raises(RuntimeError):
        transport.request("db.ping", {}, timeout_seconds=0.01, max_retries=0)
