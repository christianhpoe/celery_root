# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import base64

from django.test import Client, override_settings


def _basic_auth_header(username: str, password: str) -> str:
    token = base64.b64encode(f"{username}:{password}".encode()).decode("utf-8")
    return f"Basic {token}"


@override_settings(CELERY_ROOT_BASIC_AUTH="user:pswd")
def test_basic_auth_requires_credentials(web_client: Client) -> None:
    response = web_client.get("/")
    assert response.status_code == 401
    response = web_client.get("/", HTTP_AUTHORIZATION=_basic_auth_header("user", "pswd"))
    assert response.status_code == 200


@override_settings(CELERY_ROOT_BASIC_AUTH="user:pswd")
def test_basic_auth_exempts_healthcheck(web_client: Client) -> None:
    response = web_client.get("/healthcheck")
    assert response.status_code != 401
