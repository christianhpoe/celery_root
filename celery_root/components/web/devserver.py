# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Development web server for the Celery Root UI."""

from __future__ import annotations

import logging
import os
import threading
from socketserver import ThreadingMixIn
from typing import TYPE_CHECKING, Protocol
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server

from django.conf import settings as django_settings
from django.contrib.staticfiles.handlers import StaticFilesHandler
from django.core.wsgi import get_wsgi_application

if TYPE_CHECKING:
    from wsgiref.types import WSGIApplication


class _EventLike(Protocol):
    def wait(self, timeout: float | None = None) -> bool:  # pragma: no cover - protocol definition
        ...


class _ThreadedWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def _build_wsgi_app() -> WSGIApplication:
    application = get_wsgi_application()
    if django_settings.DEBUG:
        application = StaticFilesHandler(application)
    return application


def _frontend_url(host: str, port: int) -> str:
    display_host = host
    if host in {"0.0.0.0", "::"}:  # noqa: S104
        display_host = "127.0.0.1"
    if ":" in display_host and not display_host.startswith("["):
        display_host = f"[{display_host}]"
    return f"http://{display_host}:{port}/"


def serve(host: str, port: int, *, shutdown_event: _EventLike | None = None) -> None:
    """Serve the Django WSGI app with a lightweight dev server."""
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    application = _build_wsgi_app()
    httpd = make_server(
        host,
        port,
        application,
        server_class=_ThreadedWSGIServer,
        handler_class=WSGIRequestHandler,
    )
    logger = logging.getLogger(__name__)
    print(  # noqa: T201
        f"Frontend available at {_frontend_url(host, port)}",
    )
    if shutdown_event is not None:

        def _wait_for_shutdown() -> None:
            shutdown_event.wait()
            httpd.shutdown()

        threading.Thread(target=_wait_for_shutdown, daemon=True).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Dev server interrupted; shutting down.")
    finally:
        httpd.shutdown()
        httpd.server_close()
