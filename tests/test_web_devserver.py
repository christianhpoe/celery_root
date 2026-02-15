# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import django
import pytest
from django.contrib.staticfiles.handlers import StaticFilesHandler
from django.test.utils import override_settings

from celery_root.components.web import devserver

if TYPE_CHECKING:
    from collections.abc import Callable


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


def _simple_app(
    _environ: dict[str, object],
    start_response: Callable[[str, list[tuple[str, str]]], None],
) -> list[bytes]:
    start_response("200 OK", [])
    return [b"ok"]


def test_frontend_url_formats_hosts() -> None:
    assert devserver._frontend_url("0.0.0.0", 8000) == "http://127.0.0.1:8000/"  # noqa: S104
    assert devserver._frontend_url("::", 8000) == "http://127.0.0.1:8000/"
    assert devserver._frontend_url("::1", 8000) == "http://[::1]:8000/"
    assert devserver._frontend_url("127.0.0.1", 8000) == "http://127.0.0.1:8000/"


def test_build_wsgi_app_wraps_staticfiles(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(devserver, "get_wsgi_application", lambda: _simple_app)

    with override_settings(DEBUG=False):
        app = devserver._build_wsgi_app()
        assert app is _simple_app

    with override_settings(DEBUG=True):
        app = devserver._build_wsgi_app()
        assert isinstance(app, StaticFilesHandler)
