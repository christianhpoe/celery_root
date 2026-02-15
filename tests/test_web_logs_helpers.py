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
from django.conf import settings
from django.test import RequestFactory

from celery_root.components.web.views import logs as log_views
from celery_root.core.logging.utils import LOG_FILE_PREFIX

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


def test_log_helpers(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    file_path = log_dir / f"{LOG_FILE_PREFIX}-worker.log"
    file_path.write_text("line1\nline2\nline3\n")

    files = log_views._list_log_files(log_dir)
    assert files
    assert log_views._component_from_filename(file_path.name) == "worker"

    grouped = log_views._group_log_files(files)
    assert "worker" in grouped

    tail = log_views._read_tail(file_path, 2)
    assert "line2" in tail


def test_logs_view_json(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    file_path = log_dir / f"{LOG_FILE_PREFIX}-web.log"
    file_path.write_text("entry\n")

    monkeypatch.setattr(settings, "CELERY_ROOT_LOG_DIR", str(log_dir), raising=False)

    factory = RequestFactory()
    request = factory.get("/logs/", {"format": "json", "lines": "1"})
    response = log_views.logs(request)
    assert response.status_code == 200
