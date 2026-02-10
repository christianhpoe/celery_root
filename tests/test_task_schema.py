from __future__ import annotations

from celery_cnc.components.web.views.tasks import build_task_schemas
from tests.fixtures.app_one import app as app_one


def test_task_schema_resolves_int_annotations() -> None:
    schemas = build_task_schemas((app_one,), ["fixture.add"])
    params = schemas["fixture.add"]["params"]
    assert [param["input"] for param in params] == ["int", "int"]
