# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

import base64
import os
import sys
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

import django
import pytest
from django.conf import settings
from django.contrib.sessions.backends.signed_cookies import SessionStore
from django.http import HttpResponse
from django.test import RequestFactory

from celery_root.components.web import auth as auth_views

DUMMY_VALUE = "secret"

if TYPE_CHECKING:
    from django.http import HttpRequest


@pytest.fixture(scope="module", autouse=True)
def _django_setup() -> None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "celery_root.components.web.settings")
    if not django.apps.apps.ready:
        django.setup()


def test_basic_auth_parsing_and_check() -> None:
    raw = base64.b64encode(b"user:pass").decode("ascii")
    creds = auth_views._parse_basic_auth("user:pass")
    assert len(creds) == 1
    assert creds[0].username == "user"

    factory = RequestFactory()
    request = factory.get("/")
    request.META["HTTP_AUTHORIZATION"] = f"Basic {raw}"
    assert auth_views._check_basic_auth(request, creds)


def test_allowed_emails_and_provider_resolution() -> None:
    patterns = auth_views._parse_allowed_emails(".*@example.com,admin@domain.io")
    assert auth_views._is_email_allowed("user@example.com", patterns)
    assert not auth_views._is_email_allowed("user@other.com", patterns)

    assert auth_views._resolve_provider("github") == auth_views.ProviderKind.GITHUB
    assert auth_views._resolve_provider("") is None


def test_custom_handler_resolution() -> None:
    module = ModuleType("tests.fake_auth_module")

    def handler(_request: HttpRequest) -> bool:
        return True

    cast("Any", module).handler = handler
    sys.modules[module.__name__] = module
    resolved = auth_views._resolve_custom_handler("tests.fake_auth_module:handler")
    assert resolved is handler


def test_misc_helpers() -> None:
    assert auth_views._split_csv("a,b,,c") == ("a", "b", "c")
    assert auth_views._parse_int("42", 0) == 42
    assert auth_views._parse_int("", 7) == 7

    assert auth_views._sanitize_next("/tasks/") == "/tasks/"
    assert auth_views._sanitize_next("http://evil.com") is None

    response = auth_views._basic_auth_challenge()
    assert response.status_code == 401
    assert response["WWW-Authenticate"] == auth_views._BASIC_REALM


def test_oauth_endpoints_and_exchange(monkeypatch: pytest.MonkeyPatch) -> None:
    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=auth_views.ProviderKind.GITHUB,
        custom_handler=None,
        allowed_email_patterns=(),
        oauth2_key="key",
        oauth2_secret=DUMMY_VALUE,
        oauth2_redirect_uri="https://example.com/callback",
        okta_base_url=None,
        gitlab_allowed_groups=(),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )

    endpoints = auth_views._resolve_endpoints(config)
    assert "github" in endpoints.auth_url

    def _fake_post_form(_url: str, _payload: dict[str, str], *, headers: dict[str, str]) -> dict[str, Any]:
        _ = headers
        return {"access_token": "token"}

    monkeypatch.setattr(auth_views, "_http_post_form", _fake_post_form)

    factory = RequestFactory()
    request = factory.get("/login")
    token = auth_views._exchange_code_for_token("code", request, config)
    expected_value = "token"
    assert token == expected_value


def test_fetch_user_email(monkeypatch: pytest.MonkeyPatch) -> None:
    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=auth_views.ProviderKind.GOOGLE,
        custom_handler=None,
        allowed_email_patterns=(),
        oauth2_key="key",
        oauth2_secret=DUMMY_VALUE,
        oauth2_redirect_uri="https://example.com/callback",
        okta_base_url=None,
        gitlab_allowed_groups=(),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )

    def _fake_get_json(_url: str, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
        _ = headers
        return {"email": "user@example.com"}

    monkeypatch.setattr(auth_views, "_http_get_json", _fake_get_json)
    assert auth_views._fetch_user_email("token", config) == "user@example.com"


def test_custom_auth_flow() -> None:
    factory = RequestFactory()
    request = factory.get("/login", {"next": "/tasks/"})
    request.session = SessionStore()

    def _handler(_request: HttpRequest) -> bool:
        return True

    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=None,
        custom_handler=cast("auth_views.CustomAuthHandler", _handler),
        allowed_email_patterns=(),
        oauth2_key=None,
        oauth2_secret=None,
        oauth2_redirect_uri=None,
        okta_base_url=None,
        gitlab_allowed_groups=(),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )

    response = auth_views._handle_custom_login(request, config)
    assert response.status_code == 302

    response = auth_views._handle_custom_auth(request, config, lambda _req: HttpResponse("ok"))
    assert response.status_code == 200


def test_oauth_login_flow(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = RequestFactory()
    request = factory.get("/login")
    request.session = SessionStore()

    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=auth_views.ProviderKind.GITHUB,
        custom_handler=None,
        allowed_email_patterns=(),
        oauth2_key="key",
        oauth2_secret=DUMMY_VALUE,
        oauth2_redirect_uri="https://example.com/callback",
        okta_base_url=None,
        gitlab_allowed_groups=(),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )

    monkeypatch.setattr(auth_views, "_start_oauth", lambda _req, _cfg: HttpResponse("start"))
    response = auth_views._handle_oauth_login(request, config)
    assert response.status_code == 200

    request = factory.get("/login", {"code": "abc", "state": "state"})
    session = SessionStore()
    session[auth_views._SESSION_STATE_KEY] = "state"
    request.session = session
    monkeypatch.setattr(auth_views, "_complete_oauth_login", lambda _req, _cfg, _code: ("user@example.com", None))
    response = auth_views._handle_oauth_login(request, config)
    assert response.status_code == 302


def test_complete_oauth_login_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = RequestFactory()
    request = factory.get("/login")
    request.session = SessionStore()

    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=auth_views.ProviderKind.GITLAB,
        custom_handler=None,
        allowed_email_patterns=(),
        oauth2_key="key",
        oauth2_secret=DUMMY_VALUE,
        oauth2_redirect_uri="https://example.com/callback",
        okta_base_url=None,
        gitlab_allowed_groups=("team",),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )

    monkeypatch.setattr(auth_views, "_exchange_code_for_token", lambda *_args, **_kwargs: "token")
    monkeypatch.setattr(auth_views, "_fetch_user_email", lambda *_args, **_kwargs: None)
    email, error = auth_views._complete_oauth_login(request, config, "code")
    assert email is None
    assert error is not None

    monkeypatch.setattr(auth_views, "_fetch_user_email", lambda *_args, **_kwargs: "user@example.com")
    monkeypatch.setattr(auth_views, "_is_gitlab_group_allowed", lambda *_args, **_kwargs: False)
    email, error = auth_views._complete_oauth_login(request, config, "code")
    assert email is None
    assert error is not None


def test_okta_endpoints_missing() -> None:
    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=auth_views.ProviderKind.OKTA,
        custom_handler=None,
        allowed_email_patterns=(),
        oauth2_key="key",
        oauth2_secret=DUMMY_VALUE,
        oauth2_redirect_uri="https://example.com/callback",
        okta_base_url=None,
        gitlab_allowed_groups=(),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )
    with pytest.raises(auth_views.AuthError):
        auth_views._resolve_endpoints(config)


def test_load_auth_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "CELERY_ROOT_BASIC_AUTH", "user:pass", raising=False)
    monkeypatch.setenv("FLOWER_AUTH_PROVIDER", "github")
    config = auth_views.load_auth_config()
    assert config.basic_auth
    assert config.provider == auth_views.ProviderKind.GITHUB


def test_start_oauth_and_redirect(monkeypatch: pytest.MonkeyPatch) -> None:
    factory = RequestFactory()
    request = factory.get("/login", {"next": "/tasks/"})
    request.session = SessionStore()

    config = auth_views.AuthConfig(
        basic_auth=(),
        provider=auth_views.ProviderKind.GITHUB,
        custom_handler=None,
        allowed_email_patterns=(),
        oauth2_key="key",
        oauth2_secret=DUMMY_VALUE,
        oauth2_redirect_uri="https://example.com/callback",
        okta_base_url=None,
        gitlab_allowed_groups=(),
        gitlab_min_access_level=20,
        gitlab_oauth_domain="https://gitlab.com",
        error=None,
    )

    endpoint_url = "https://auth.example/token"
    monkeypatch.setattr(
        auth_views,
        "_resolve_endpoints",
        lambda _cfg: auth_views.OAuthEndpoints(
            auth_url="https://auth.example/authorize",
            token_url=endpoint_url,
            user_info_url="https://auth.example/user",
            scopes=("email",),
        ),
    )

    response = auth_views._start_oauth(request, config)
    assert response.status_code == 302
    assert auth_views._SESSION_STATE_KEY in request.session

    assert auth_views._validate_state(request, request.session[auth_views._SESSION_STATE_KEY])

    request.session[auth_views._SESSION_NEXT_KEY] = "/tasks/"
    redirect = auth_views._redirect_after_login(request)
    assert redirect.status_code == 302
