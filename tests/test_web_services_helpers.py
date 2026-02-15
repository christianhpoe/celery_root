# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, cast

import django
import pytest
from django.conf import settings

from celery_root.components.web import services
from tests.fixtures import app_one

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


def test_split_path_and_load_app() -> None:
    module_path, attr = services._split_path("tests.fixtures.app_one:app")
    assert module_path.endswith("tests.fixtures.app_one")
    assert attr == "app"

    app = services._load_app("tests.fixtures.app_one:app")
    assert cast("Any", app).main == cast("Any", app_one.app).main


def test_app_name_and_task_names() -> None:
    name = services.app_name(app_one.app)
    assert name

    task_names = services.list_task_names((app_one.app,))
    assert "fixture.add" in task_names


def test_registry_and_worker_options(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CELERY_ROOT_WORKERS", ["tests.fixtures.app_one:app"], raising=False)
    registry = services.get_registry()
    assert registry.get_apps()

    options = services.list_worker_options()
    assert options


def test_db_path_and_retention(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(settings, "CELERY_ROOT_DB_PATH", str(tmp_path / "db.sqlite"), raising=False)
    monkeypatch.setattr(settings, "CELERY_ROOT_RETENTION_DAYS", 3, raising=False)
    assert services.db_path().name == "db.sqlite"
    assert services.retention_days() == 3
