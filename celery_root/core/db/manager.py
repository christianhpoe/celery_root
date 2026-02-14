# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""DB manager process for handling RPC requests."""

from __future__ import annotations

import logging
import threading
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from multiprocessing import Event, Process
from multiprocessing.connection import Client, Listener
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from celery_root.config import set_settings
from celery_root.core.db.adapters.memory import MemoryController
from celery_root.core.db.adapters.sqlite import SQLiteController
from celery_root.core.db.dispatch import RPC_OPERATIONS
from celery_root.core.logging.setup import configure_process_logging
from celery_root.shared.schemas import RPC_SCHEMA_VERSION, RpcError, RpcRequestEnvelope, RpcResponseEnvelope

if TYPE_CHECKING:
    from collections.abc import Callable
    from multiprocessing.connection import Connection

    from celery_root.config import CeleryRootConfig
    from celery_root.core.db.adapters.base import BaseDBController
    from celery_root.core.db.dispatch import RpcOperation

_DB_KIND_MEMORY = 0
_DB_KIND_SQLITE = 1


@dataclass(frozen=True, slots=True)
class _ErrorContext:
    request_id: str
    op: str
    duration_ms: float


def _authkey_from_config(config: CeleryRootConfig) -> bytes | None:
    auth = config.database.rpc_auth_key
    if not auth:
        return None
    return auth.encode("utf-8")


def _build_backend(
    config: CeleryRootConfig,
    controller_factory: Callable[[], BaseDBController] | None,
) -> BaseDBController:
    if controller_factory is not None:
        return controller_factory()
    db_kind = config.database.db_kind
    if db_kind == _DB_KIND_MEMORY:
        return MemoryController()
    if db_kind == _DB_KIND_SQLITE:
        return SQLiteController(config.database.db_path)
    msg = f"Unsupported db_kind {db_kind}"
    raise RuntimeError(msg)


class DBManager(Process):
    """DB manager process hosting the RPC server."""

    def __init__(
        self,
        config: CeleryRootConfig,
        controller_factory: Callable[[], BaseDBController] | None = None,
    ) -> None:
        """Create a DB manager process."""
        super().__init__(daemon=True)
        self._config = config
        self._controller_factory = controller_factory
        self._stop_event = Event()
        self._logger = logging.getLogger(__name__)
        self._address = (config.database.rpc_host, config.database.rpc_port)
        self._authkey = _authkey_from_config(config)

    def stop(self) -> None:
        """Signal the DB manager to stop."""
        self._stop_event.set()
        with suppress(Exception):  # pragma: no cover - best effort
            conn = Client(self._address, authkey=self._authkey)
            conn.close()

    def run(self) -> None:
        """Run the RPC server loop."""
        set_settings(self._config)
        configure_process_logging(self._config, component="db_manager")
        self._logger.info("DBManager starting.")
        controller = _build_backend(self._config, self._controller_factory)
        controller.initialize()
        controller.ensure_schema()

        try:
            self._serve(controller)
        except KeyboardInterrupt:
            self._logger.info("DBManager interrupted; shutting down.")
        finally:
            controller.close()
            self._logger.info("DBManager stopped.")

    def _serve(self, controller: BaseDBController) -> None:
        lock = threading.Lock()
        inflight = threading.BoundedSemaphore(self._config.database.rpc_max_inflight)
        listener = Listener(self._address, authkey=self._authkey)
        self._logger.info("DBManager listening on %s:%s", *self._address)

        def _watch_stop() -> None:
            self._stop_event.wait()
            with suppress(Exception):
                listener.close()

        threading.Thread(target=_watch_stop, daemon=True).start()

        while not self._stop_event.is_set():
            try:
                conn = listener.accept()
            except OSError:
                break
            threading.Thread(
                target=self._handle_connection,
                args=(conn, controller, lock, inflight),
                daemon=True,
            ).start()

    def _handle_connection(
        self,
        conn: Connection,
        controller: BaseDBController,
        lock: threading.Lock,
        inflight: threading.BoundedSemaphore,
    ) -> None:
        with conn:
            while not self._stop_event.is_set():
                try:
                    data = conn.recv_bytes()
                except EOFError:
                    break
                if not inflight.acquire(blocking=False):
                    response = self._error_response(
                        request_id=uuid.uuid4().hex,
                        code="BUSY",
                        message="DB manager is busy",
                    )
                    conn.send_bytes(response)
                    continue
                try:
                    response = self._dispatch(data, controller, lock)
                finally:
                    inflight.release()
                with suppress(Exception):
                    conn.send_bytes(response)

    def _dispatch(
        self,
        data: bytes,
        controller: BaseDBController,
        lock: threading.Lock,
    ) -> bytes:
        request_id = uuid.uuid4().hex
        op = "unknown"
        start = time.monotonic()
        max_bytes = self._config.database.rpc_max_message_bytes

        try:
            if len(data) > max_bytes:
                return self._error_response(
                    request_id=request_id,
                    code="MESSAGE_TOO_LARGE",
                    message="RPC request exceeded max message size",
                )
            envelope = RpcRequestEnvelope.model_validate_json(data)
            request_id = envelope.request_id
            op = envelope.op
            self._logger.debug(
                "DB RPC recv request_id=%s op=%s client=%s bytes=%d",
                request_id,
                op,
                envelope.client,
                len(data),
            )
            if envelope.schema_version != RPC_SCHEMA_VERSION:
                return self._error_response(
                    request_id=request_id,
                    code="SCHEMA_UNSUPPORTED",
                    message="Unsupported RPC schema version",
                )
            operation = RPC_OPERATIONS.get(op)
            if operation is None:
                return self._error_response(
                    request_id=request_id,
                    code="OP_NOT_FOUND",
                    message=f"Unknown operation: {op}",
                )
            response_payload = self._handle_operation(operation, envelope.payload, controller, lock)
            response = RpcResponseEnvelope(
                request_id=request_id,
                ok=True,
                payload=response_payload,
                timestamp=datetime.now(UTC),
            )
            response_bytes = response.model_dump_json().encode("utf-8")
            self._logger.debug(
                "DB RPC ok request_id=%s op=%s payload_bytes=%s",
                request_id,
                op,
                len(response_bytes),
            )
            duration_ms = (time.monotonic() - start) * 1000.0
            self._logger.info(
                "DB RPC %s %s ok duration_ms=%.1f",
                request_id,
                op,
                duration_ms,
            )
        except ValidationError as exc:
            duration_ms = (time.monotonic() - start) * 1000.0
            context = _ErrorContext(request_id=request_id, op=op, duration_ms=duration_ms)
            return self._handle_error(context, "VALIDATION_ERROR", "Invalid RPC payload", exc)
        except Exception as exc:  # pragma: no cover - defensive  # noqa: BLE001
            duration_ms = (time.monotonic() - start) * 1000.0
            context = _ErrorContext(request_id=request_id, op=op, duration_ms=duration_ms)
            return self._handle_error(context, "SERVER_ERROR", "RPC handler failed", exc)
        else:
            return response_bytes

    def _handle_operation(
        self,
        operation: RpcOperation[Any, Any],
        payload: dict[str, Any] | list[Any] | None,
        controller: BaseDBController,
        lock: threading.Lock,
    ) -> dict[str, Any] | list[Any] | None:
        payload_dict = payload if isinstance(payload, dict) else {}
        request_model = operation.request_model.model_validate(payload_dict)
        with lock:
            response_model = operation.handler(controller, request_model)
        return response_model.model_dump(mode="json")

    def _handle_error(
        self,
        context: _ErrorContext,
        code: str,
        message: str,
        exc: Exception,
    ) -> bytes:
        details: dict[str, Any] | None = None
        if isinstance(exc, ValidationError):
            details = {"errors": exc.errors()}
        error = RpcError(code=code, message=message, details=details)
        self._logger.info(
            "DB RPC %s %s error=%s duration_ms=%.1f",
            context.request_id,
            context.op,
            code,
            context.duration_ms,
        )
        response = RpcResponseEnvelope(
            request_id=context.request_id,
            ok=False,
            error=error,
            timestamp=datetime.now(UTC),
        )
        return response.model_dump_json().encode("utf-8")

    @staticmethod
    def _error_response(request_id: str, code: str, message: str) -> bytes:
        response = RpcResponseEnvelope(
            request_id=request_id,
            ok=False,
            error=RpcError(code=code, message=message),
            timestamp=datetime.now(UTC),
        )
        return response.model_dump_json().encode("utf-8")
