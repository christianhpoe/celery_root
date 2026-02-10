"""Authentication helpers for the Celery CnC web app."""

from __future__ import annotations

import base64
import importlib
import json
import os
import re
import secrets
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, NoReturn, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Callable

from django.conf import settings
from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseBadRequest,
    HttpResponseForbidden,
    HttpResponseNotFound,
    HttpResponseRedirect,
)
from django.urls import reverse

_SESSION_AUTH_KEY = "celery_cnc_auth"
_SESSION_STATE_KEY = "celery_cnc_oauth_state"
_SESSION_NEXT_KEY = "celery_cnc_oauth_next"
_BASIC_REALM = 'Basic realm="Celery CnC"'
_DEFAULT_GITLAB_DOMAIN = "https://gitlab.com"
_DEFAULT_GITLAB_MIN_LEVEL = 20
_EXEMPT_PATHS = frozenset({"/healthcheck", "/metrics", "/login", "/logout", "/favicon.ico"})


class ProviderKind(Enum):
    """Supported OAuth providers."""

    GOOGLE = "google"
    GITHUB = "github"
    GITLAB = "gitlab"
    OKTA = "okta"


class AuthError(RuntimeError):
    """Raised when OAuth authentication fails."""

    EMAIL_MISSING = "OAuth provider did not return an email."
    EMAIL_NOT_ALLOWED = "Email address is not allowed."
    GITLAB_GROUP_DENIED = "GitLab group membership check failed."
    OKTA_BASE_URL_MISSING = "FLOWER_OAUTH2_OKTA_BASE_URL is not configured."
    UNSUPPORTED_PROVIDER = "Unsupported OAuth provider."
    TOKEN_MISSING = "OAuth token response missing access_token."  # noqa: S105 - error message, not a secret.
    UNEXPECTED_JSON = "Unexpected JSON payload."
    CUSTOM_HANDLER_NOT_CALLABLE = "Custom auth handler is not callable: {raw}"

    @classmethod
    def email_missing(cls) -> AuthError:
        """Return an AuthError for missing OAuth emails."""
        return cls(cls.EMAIL_MISSING)

    @classmethod
    def email_not_allowed(cls) -> AuthError:
        """Return an AuthError for blocked email addresses."""
        return cls(cls.EMAIL_NOT_ALLOWED)

    @classmethod
    def gitlab_group_denied(cls) -> AuthError:
        """Return an AuthError for disallowed GitLab group membership."""
        return cls(cls.GITLAB_GROUP_DENIED)

    @classmethod
    def okta_base_url_missing(cls) -> AuthError:
        """Return an AuthError for missing Okta base URL configuration."""
        return cls(cls.OKTA_BASE_URL_MISSING)

    @classmethod
    def unsupported_provider(cls) -> AuthError:
        """Return an AuthError for unsupported OAuth providers."""
        return cls(cls.UNSUPPORTED_PROVIDER)

    @classmethod
    def token_missing(cls) -> AuthError:
        """Return an AuthError for missing token responses."""
        return cls(cls.TOKEN_MISSING)

    @classmethod
    def unexpected_json(cls) -> AuthError:
        """Return an AuthError for unexpected JSON responses."""
        return cls(cls.UNEXPECTED_JSON)

    @classmethod
    def custom_handler_not_callable(cls, raw: str) -> AuthError:
        """Return an AuthError for invalid custom auth handlers."""
        message = cls.CUSTOM_HANDLER_NOT_CALLABLE.format(raw=raw)
        return cls(message)


@dataclass(frozen=True, slots=True)
class BasicCredential:
    """Username/password pair for basic auth."""

    username: str
    password: str


@dataclass(frozen=True, slots=True)
class OAuthEndpoints:
    """OAuth endpoints for a provider."""

    auth_url: str
    token_url: str
    user_info_url: str
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AuthConfig:
    """Resolved authentication configuration."""

    basic_auth: tuple[BasicCredential, ...]
    provider: ProviderKind | None
    custom_handler: CustomAuthHandler | None
    allowed_email_patterns: tuple[re.Pattern[str], ...]
    oauth2_key: str | None
    oauth2_secret: str | None
    oauth2_redirect_uri: str | None
    okta_base_url: str | None
    gitlab_allowed_groups: tuple[str, ...]
    gitlab_min_access_level: int
    gitlab_oauth_domain: str
    error: str | None

    @property
    def enabled(self) -> bool:
        """Return True if any auth mechanism is configured."""
        return bool(self.basic_auth or self.provider is not None or self.custom_handler or self.error)

    @property
    def oauth_enabled(self) -> bool:
        """Return True if OAuth is configured."""
        return self.provider is not None

    @property
    def custom_enabled(self) -> bool:
        """Return True if a custom auth handler is configured."""
        return self.custom_handler is not None


class CustomAuthHandler(Protocol):
    """Callable used for custom authentication logic."""

    def __call__(self, request: HttpRequest) -> bool | HttpResponse:
        """Return True to allow, False to deny, or an HttpResponse to short-circuit."""
        ...


class AuthMiddleware:
    """Middleware enforcing Flower-compatible authentication."""

    def __init__(self, get_response: Callable[[HttpRequest], HttpResponse]) -> None:
        """Store the next response handler."""
        self.get_response = get_response

    def __call__(self, request: HttpRequest) -> HttpResponse:
        """Evaluate auth checks and return the response."""
        config = load_auth_config()
        if config.error:
            return HttpResponse(config.error, status=500)
        if not config.enabled or _is_exempt_path(request.path):
            return self.get_response(request)

        if config.basic_auth and _check_basic_auth(request, config.basic_auth):
            return self.get_response(request)

        if config.custom_enabled:
            return _handle_custom_auth(request, config, self.get_response)

        if config.oauth_enabled:
            return self.get_response(request) if _is_authenticated(request) else _redirect_to_login(request)

        return _basic_auth_challenge()


def login(request: HttpRequest) -> HttpResponse:
    """Initiate an OAuth flow or handle the callback."""
    config = load_auth_config()
    if config.custom_enabled:
        return _handle_custom_login(request, config)
    if not config.oauth_enabled:
        return HttpResponseNotFound("OAuth provider is not configured.")
    return _handle_oauth_login(request, config)


def logout(request: HttpRequest) -> HttpResponse:
    """Clear OAuth session information."""
    if hasattr(request, "session"):
        request.session.flush()
    return HttpResponseRedirect("/")


def _handle_custom_auth(
    request: HttpRequest,
    config: AuthConfig,
    get_response: Callable[[HttpRequest], HttpResponse],
) -> HttpResponse:
    handler = config.custom_handler
    if handler is None:
        return HttpResponse("Custom auth handler missing.", status=500)
    result = handler(request)
    if isinstance(result, HttpResponse):
        return result
    if result:
        return get_response(request)
    return HttpResponseForbidden("Authentication required.")


def _handle_custom_login(request: HttpRequest, config: AuthConfig) -> HttpResponse:
    handler = config.custom_handler
    if handler is None:
        return HttpResponse("Custom auth handler missing.", status=500)
    next_param = _sanitize_next(request.GET.get("next"))
    if next_param:
        request.session[_SESSION_NEXT_KEY] = next_param
    result = handler(request)
    if isinstance(result, HttpResponse):
        return result
    if result:
        return _redirect_after_login(request)
    return HttpResponseForbidden("Authentication required.")


def _handle_oauth_login(request: HttpRequest, config: AuthConfig) -> HttpResponse:
    if "error" in request.GET:
        error = request.GET.get("error", "oauth_error")
        return HttpResponseBadRequest(f"OAuth error: {error}")

    code = request.GET.get("code")
    if code is None:
        return _start_oauth(request, config)

    state = request.GET.get("state")
    if not state or not _validate_state(request, state):
        return HttpResponseBadRequest("Invalid OAuth state.")

    email, error_response = _complete_oauth_login(request, config, code)
    if error_response is not None or email is None:
        return error_response or HttpResponseForbidden("Authentication required.")

    _store_login(request, email, config.provider.value if config.provider else "oauth")
    return _redirect_after_login(request)


def _raise_auth_error(error: AuthError) -> NoReturn:
    raise error


def _complete_oauth_login(
    request: HttpRequest,
    config: AuthConfig,
    code: str,
) -> tuple[str | None, HttpResponse | None]:
    try:
        token = _exchange_code_for_token(code, request, config)
        email = _fetch_user_email(token, config)
        if email is None:
            _raise_auth_error(AuthError.email_missing())
        if not _is_email_allowed(email, config.allowed_email_patterns):
            _raise_auth_error(AuthError.email_not_allowed())
        if (
            config.provider is ProviderKind.GITLAB
            and config.gitlab_allowed_groups
            and not _is_gitlab_group_allowed(token, config)
        ):
            _raise_auth_error(AuthError.gitlab_group_denied())
    except AuthError as exc:
        return None, HttpResponseForbidden(str(exc))
    return email, None


def load_auth_config() -> AuthConfig:
    """Load auth configuration from Django settings and Flower env vars."""
    raw_basic = _get_setting(
        "CELERY_CNC_BASIC_AUTH",
        ("FLOWER_BASIC_AUTH", "FLOWER_BASIC_AUTHENTICATION", "FLOWER_BASIC_AUTHORIZATION"),
    )
    raw_provider = _get_setting("CELERY_CNC_AUTH_PROVIDER", ("FLOWER_AUTH_PROVIDER",))
    raw_auth = _get_setting("CELERY_CNC_AUTH", ("FLOWER_AUTH",))
    oauth2_key = _get_setting("CELERY_CNC_OAUTH2_KEY", ("FLOWER_OAUTH2_KEY",))
    oauth2_secret = _get_setting("CELERY_CNC_OAUTH2_SECRET", ("FLOWER_OAUTH2_SECRET",))
    oauth2_redirect = _get_setting("CELERY_CNC_OAUTH2_REDIRECT_URI", ("FLOWER_OAUTH2_REDIRECT_URI",))
    okta_base_url = _get_setting(
        "CELERY_CNC_OAUTH2_OKTA_BASE_URL",
        ("FLOWER_OAUTH2_OKTA_BASE_URL",),
    )
    gitlab_groups = _get_setting(
        "CELERY_CNC_GITLAB_AUTH_ALLOWED_GROUPS",
        ("FLOWER_GITLAB_AUTH_ALLOWED_GROUPS",),
    )
    gitlab_min_level = _get_setting(
        "CELERY_CNC_GITLAB_MIN_ACCESS_LEVEL",
        ("FLOWER_GITLAB_MIN_ACCESS_LEVEL",),
    )
    gitlab_domain = _get_setting(
        "CELERY_CNC_GITLAB_OAUTH_DOMAIN",
        ("FLOWER_GITLAB_OAUTH_DOMAIN",),
    )

    basic_auth = _parse_basic_auth(raw_basic)
    error: str | None = None
    provider = _resolve_provider(raw_provider)
    custom_handler: CustomAuthHandler | None = None
    if provider is None:
        try:
            custom_handler = _resolve_custom_handler(raw_provider)
        except AuthError as exc:
            error = str(exc)
    if raw_provider and provider is None and custom_handler is None and error is None:
        error = f"Unsupported auth provider: {raw_provider}"
    allowed_email_patterns = _parse_allowed_emails(raw_auth)
    gitlab_allowed_groups = _split_csv(gitlab_groups)
    gitlab_min_access_level = _parse_int(gitlab_min_level, _DEFAULT_GITLAB_MIN_LEVEL)
    gitlab_oauth_domain = gitlab_domain or _DEFAULT_GITLAB_DOMAIN

    return AuthConfig(
        basic_auth=basic_auth,
        provider=provider,
        custom_handler=custom_handler,
        allowed_email_patterns=allowed_email_patterns,
        oauth2_key=oauth2_key,
        oauth2_secret=oauth2_secret,
        oauth2_redirect_uri=oauth2_redirect,
        okta_base_url=okta_base_url,
        gitlab_allowed_groups=gitlab_allowed_groups,
        gitlab_min_access_level=gitlab_min_access_level,
        gitlab_oauth_domain=gitlab_oauth_domain,
        error=error,
    )


def _parse_basic_auth(raw: str | None) -> tuple[BasicCredential, ...]:
    if not raw:
        return ()
    entries = []
    for raw_item in raw.split(","):
        item = raw_item.strip()
        if not item:
            continue
        if ":" not in item:
            continue
        username, password = item.split(":", 1)
        entries.append(BasicCredential(username=username, password=password))
    return tuple(entries)


def _parse_allowed_emails(raw: str | None) -> tuple[re.Pattern[str], ...]:
    if not raw:
        return ()
    value = raw.strip()
    if not value:
        return ()
    if value.startswith("allowed-emails"):
        value = value[len("allowed-emails") :].lstrip(":=. ")
    patterns = [item.strip() for item in value.split(",") if item.strip()]
    if not patterns:
        return ()
    return tuple(re.compile(pattern) for pattern in patterns)


def _split_csv(raw: str | None) -> tuple[str, ...]:
    if not raw:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())


def _parse_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _resolve_provider(raw: str | None) -> ProviderKind | None:
    if not raw:
        return None
    value = raw.strip()
    lowered = value.lower()
    direct = {
        "google": ProviderKind.GOOGLE,
        "github": ProviderKind.GITHUB,
        "gitlab": ProviderKind.GITLAB,
        "okta": ProviderKind.OKTA,
    }
    if lowered in direct:
        return direct[lowered]
    if lowered.startswith(("flower.views.auth", "flower.")):
        for key, provider in direct.items():
            if key in lowered:
                return provider
    return None


def _resolve_custom_handler(raw: str | None) -> CustomAuthHandler | None:
    if not raw:
        return None
    if raw.strip().lower() in {"google", "github", "gitlab", "okta"}:
        return None
    if "." not in raw and ":" not in raw:
        return None
    module_path, attr = _split_path(raw)
    module = importlib.import_module(module_path)
    handler = getattr(module, attr, None)
    if handler is None or not callable(handler):
        raise AuthError.custom_handler_not_callable(raw)
    return cast("CustomAuthHandler", handler)


def _split_path(path: str) -> tuple[str, str]:
    if ":" in path:
        module_path, attr = path.split(":", 1)
        return module_path, attr
    module_path, attr = path.rsplit(".", 1)
    return module_path, attr


def _get_setting(key: str, env_keys: tuple[str, ...]) -> str | None:
    value = getattr(settings, key, None)
    if isinstance(value, str) and value.strip():
        return value.strip()
    if value is not None:
        return str(value)
    for env_key in env_keys:
        env_value = os.getenv(env_key)
        if env_value and env_value.strip():
            return env_value.strip()
    return None


def _is_exempt_path(path: str) -> bool:
    normalized = path.rstrip("/") or "/"
    if normalized in _EXEMPT_PATHS:
        return True
    static_url = getattr(settings, "STATIC_URL", "/static/")
    return bool(static_url) and path.startswith(static_url)


def _check_basic_auth(request: HttpRequest, creds: tuple[BasicCredential, ...]) -> bool:
    header = request.META.get("HTTP_AUTHORIZATION")
    if not header:
        return False
    if not header.lower().startswith("basic "):
        return False
    encoded = header.split(" ", 1)[1].strip()
    try:
        decoded = base64.b64decode(encoded).decode("utf-8")
    except (UnicodeDecodeError, ValueError):
        return False
    if ":" not in decoded:
        return False
    username, password = decoded.split(":", 1)
    for cred in creds:
        if secrets.compare_digest(username, cred.username) and secrets.compare_digest(password, cred.password):
            return True
    return False


def _basic_auth_challenge() -> HttpResponse:
    response = HttpResponse("Authentication required.", status=401)
    response["WWW-Authenticate"] = _BASIC_REALM
    return response


def _is_authenticated(request: HttpRequest) -> bool:
    if not hasattr(request, "session"):
        return False
    session = request.session
    return bool(session.get(_SESSION_AUTH_KEY))


def _redirect_to_login(request: HttpRequest) -> HttpResponse:
    login_url = reverse("login")
    next_param = _sanitize_next(request.get_full_path())
    if next_param:
        login_url = f"{login_url}?{urllib.parse.urlencode({'next': next_param})}"
    return HttpResponseRedirect(login_url)


def _sanitize_next(candidate: str | None) -> str | None:
    if not candidate:
        return None
    if candidate.startswith("/") and not candidate.startswith("//"):
        return candidate
    return None


def _start_oauth(request: HttpRequest, config: AuthConfig) -> HttpResponse:
    if not config.oauth2_key or not config.oauth2_secret:
        return HttpResponseBadRequest("OAuth client credentials are not configured.")
    try:
        endpoints = _resolve_endpoints(config)
    except AuthError as exc:
        return HttpResponseBadRequest(str(exc))
    state = secrets.token_urlsafe(16)
    request.session[_SESSION_STATE_KEY] = state
    next_param = _sanitize_next(request.GET.get("next"))
    if next_param:
        request.session[_SESSION_NEXT_KEY] = next_param
    redirect_uri = _resolve_redirect_uri(request, config)
    query = {
        "response_type": "code",
        "client_id": config.oauth2_key,
        "redirect_uri": redirect_uri,
        "scope": " ".join(endpoints.scopes),
        "state": state,
    }
    url = f"{endpoints.auth_url}?{urllib.parse.urlencode(query)}"
    return HttpResponseRedirect(url)


def _validate_state(request: HttpRequest, state: str) -> bool:
    stored = request.session.get(_SESSION_STATE_KEY)
    if stored is None:
        return False
    if not secrets.compare_digest(str(stored), state):
        return False
    request.session.pop(_SESSION_STATE_KEY, None)
    return True


def _resolve_redirect_uri(request: HttpRequest, config: AuthConfig) -> str:
    if config.oauth2_redirect_uri:
        return config.oauth2_redirect_uri
    return request.build_absolute_uri(reverse("login"))


def _resolve_endpoints(config: AuthConfig) -> OAuthEndpoints:
    if config.provider is ProviderKind.GOOGLE:
        return OAuthEndpoints(
            auth_url="https://accounts.google.com/o/oauth2/v2/auth",
            token_url="https://oauth2.googleapis.com/token",  # noqa: S106 - OAuth endpoint.
            user_info_url="https://openidconnect.googleapis.com/v1/userinfo",
            scopes=("openid", "email", "profile"),
        )
    if config.provider is ProviderKind.GITHUB:
        return OAuthEndpoints(
            auth_url="https://github.com/login/oauth/authorize",
            token_url="https://github.com/login/oauth/access_token",  # noqa: S106 - OAuth endpoint.
            user_info_url="https://api.github.com/user",
            scopes=("read:user", "user:email"),
        )
    if config.provider is ProviderKind.GITLAB:
        domain = config.gitlab_oauth_domain.rstrip("/")
        return OAuthEndpoints(
            auth_url=f"{domain}/oauth/authorize",
            token_url=f"{domain}/oauth/token",
            user_info_url=f"{domain}/api/v4/user",
            scopes=("read_user", "read_api"),
        )
    if config.provider is ProviderKind.OKTA:
        if not config.okta_base_url:
            raise AuthError.okta_base_url_missing()
        base = config.okta_base_url.rstrip("/")
        return OAuthEndpoints(
            auth_url=f"{base}/v1/authorize",
            token_url=f"{base}/v1/token",
            user_info_url=f"{base}/v1/userinfo",
            scopes=("openid", "email", "profile"),
        )
    raise AuthError.unsupported_provider()


def _exchange_code_for_token(code: str, request: HttpRequest, config: AuthConfig) -> str:
    endpoints = _resolve_endpoints(config)
    redirect_uri = _resolve_redirect_uri(request, config)
    payload = {
        "code": code,
        "client_id": config.oauth2_key or "",
        "client_secret": config.oauth2_secret or "",
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }
    headers = {"Accept": "application/json"}
    data = _http_post_form(endpoints.token_url, payload, headers=headers)
    token = data.get("access_token")
    if not token:
        raise AuthError.token_missing()
    return str(token)


def _fetch_user_email(token: str, config: AuthConfig) -> str | None:
    endpoints = _resolve_endpoints(config)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    profile = _http_get_json(endpoints.user_info_url, headers=headers)
    if config.provider is ProviderKind.GITHUB:
        return _extract_github_email(profile, headers)
    return _extract_profile_email(profile)


def _extract_profile_email(profile: dict[str, Any]) -> str | None:
    email = profile.get("email")
    return str(email) if email else None


def _extract_github_email(profile: dict[str, Any], headers: dict[str, str]) -> str | None:
    email = _extract_profile_email(profile)
    if email:
        return email
    emails, _ = _http_get_json_with_headers("https://api.github.com/user/emails", headers=headers)
    if not isinstance(emails, list):
        return None
    primary = next(
        (item for item in emails if item.get("primary") and item.get("verified")),
        None,
    )
    if primary and primary.get("email"):
        return str(primary["email"])
    if emails and emails[0].get("email"):
        return str(emails[0]["email"])
    return None


def _is_email_allowed(email: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    if not patterns:
        return True
    return any(pattern.match(email) for pattern in patterns)


def _is_gitlab_group_allowed(token: str, config: AuthConfig) -> bool:
    groups = _fetch_gitlab_groups(token, config)
    if not groups:
        return False
    allowed = set(config.gitlab_allowed_groups)
    return any(group in groups for group in allowed)


def _fetch_gitlab_groups(token: str, config: AuthConfig) -> set[str]:
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    domain = config.gitlab_oauth_domain.rstrip("/")
    groups: set[str] = set()
    page = 1
    while True:
        query = urllib.parse.urlencode(
            {
                "min_access_level": config.gitlab_min_access_level,
                "per_page": 100,
                "page": page,
            },
        )
        url = f"{domain}/api/v4/groups?{query}"
        data, response_headers = _http_get_json_with_headers(url, headers=headers)
        if isinstance(data, list):
            for item in data:
                full_path = item.get("full_path")
                path = item.get("path")
                if full_path:
                    groups.add(str(full_path))
                if path:
                    groups.add(str(path))
        next_page = response_headers.get("X-Next-Page")
        if not next_page:
            break
        try:
            page = int(next_page)
        except ValueError:
            break
    return groups


def _store_login(request: HttpRequest, email: str, provider: str) -> None:
    request.session[_SESSION_AUTH_KEY] = {
        "email": email,
        "provider": provider,
        "authenticated_at": time.time(),
    }


def _redirect_after_login(request: HttpRequest) -> HttpResponse:
    next_url = request.session.pop(_SESSION_NEXT_KEY, None)
    if isinstance(next_url, str) and next_url:
        return HttpResponseRedirect(next_url)
    return HttpResponseRedirect("/")


def _http_post_form(
    url: str,
    payload: dict[str, str],
    *,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    data = urllib.parse.urlencode(payload).encode("utf-8")
    request_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(  # noqa: S310 - trusted OAuth endpoints.
        url,
        data=data,
        headers=request_headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - trusted OAuth endpoints
        body = response.read().decode("utf-8")
        content_type = response.headers.get("Content-Type", "")
        if "application/json" in content_type or body.strip().startswith("{"):
            return json.loads(body)
        return dict(urllib.parse.parse_qsl(body))


def _http_get_json(url: str, *, headers: dict[str, str] | None = None) -> dict[str, Any]:
    data, _ = _http_get_json_with_headers(url, headers=headers)
    if isinstance(data, dict):
        return data
    raise AuthError.unexpected_json()


def _http_get_json_with_headers(
    url: str,
    *,
    headers: dict[str, str] | None = None,
) -> tuple[Any, dict[str, str]]:
    request = urllib.request.Request(url, headers=headers or {}, method="GET")  # noqa: S310 - trusted OAuth endpoints.
    with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310 - trusted OAuth endpoints
        body = response.read().decode("utf-8")
        payload = json.loads(body)
        return payload, dict(response.headers)
