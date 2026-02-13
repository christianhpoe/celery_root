# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

from celery_root.core.registry import WorkerRegistry
from tests.fixtures.app_one import app as app_one


def test_register_app_instance() -> None:
    registry = WorkerRegistry([app_one])
    apps = registry.get_apps()
    assert len(apps) == 1
    assert apps[0] is app_one


def test_register_by_import_path() -> None:
    registry = WorkerRegistry(["tests.fixtures.app_one:app"])
    apps = registry.get_apps()
    assert len(apps) == 1


def test_register_by_import_path_outside_repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module_path = tmp_path / "external_app.py"
    module_path.write_text(
        "from celery import Celery\napp = Celery('external', broker='memory://', backend='cache+memory://')\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    registry = WorkerRegistry(["external_app:app"])
    apps = registry.get_apps()
    assert len(apps) == 1
