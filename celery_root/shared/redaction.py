# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Helpers for redacting sensitive connection details."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED_VALUE = "***"

_PASSWORD_KEYS = ("password", "passwd", "pwd")
_PASSWORD_PARAM_RE = re.compile(r"(?i)(password|passwd|pwd)=([^&;\s]+)")
_URL_CREDENTIAL_RE = re.compile(r"(?i)([a-z][a-z0-9+.-]*://)([^\s/@:]*):([^\s/@]*)@")


def redact_url_password(value: str | None) -> str | None:
    """Redact passwords embedded in connection URLs or query strings."""
    if value is None:
        return None
    text = str(value)
    if not text or REDACTED_VALUE in text:
        return text
    if "://" not in text:
        return _redact_password_params(text) if _contains_password_param(text) else text
    return _redact_url_text(text)


def redact_access_data(value: object) -> object:
    """Recursively redact password fields and connection strings."""
    result: object = value
    if isinstance(value, str):
        result = redact_url_password(value)
    elif isinstance(value, Mapping):
        result = _redact_mapping(value)
    elif isinstance(value, list):
        result = [redact_access_data(item) for item in value]
    elif isinstance(value, tuple):
        result = tuple(redact_access_data(item) for item in value)
    elif isinstance(value, set):
        result = {redact_access_data(item) for item in value}
    elif isinstance(value, Sequence):
        result = [redact_access_data(item) for item in value]
    return result


def _redact_mapping(value: Mapping[object, object]) -> dict[object, object]:
    redacted: dict[object, object] = {}
    for key, item in value.items():
        if isinstance(key, str) and _is_password_key(key):
            redacted[key] = REDACTED_VALUE
        else:
            redacted[key] = redact_access_data(item)
    return redacted


def _is_password_key(key: str) -> bool:
    normalized = key.strip().lower().replace("-", "_")
    if normalized in _PASSWORD_KEYS:
        return True
    return any(normalized.endswith(suffix) for suffix in _PASSWORD_KEYS)


def _contains_password_param(text: str) -> bool:
    lowered = text.lower()
    return any(f"{token}=" in lowered for token in _PASSWORD_KEYS)


def _redact_password_params(text: str) -> str:
    return _PASSWORD_PARAM_RE.sub(lambda match: f"{match.group(1)}={REDACTED_VALUE}", text)


def _redact_query(query: str) -> tuple[str, bool]:
    items = parse_qsl(query, keep_blank_values=True)
    if not items:
        return query, False
    redacted = False
    updated: list[tuple[str, str]] = []
    for key, value in items:
        if _is_password_key(key):
            updated.append((key, REDACTED_VALUE))
            redacted = True
        else:
            updated.append((key, value))
    return urlencode(updated, doseq=True), redacted


def _redact_url_credentials_in_text(text: str) -> str:
    return _URL_CREDENTIAL_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}:{REDACTED_VALUE}@",
        text,
    )


def _redact_url_text(text: str) -> str:
    try:
        parts = urlsplit(text)
    except ValueError:
        return _redact_password_params(_redact_url_credentials_in_text(text))

    if not parts.scheme or not parts.netloc:
        redacted_text = _redact_url_credentials_in_text(text)
        return _redact_password_params(redacted_text) if _contains_password_param(redacted_text) else redacted_text

    netloc, netloc_redacted = _redact_netloc(parts.netloc)
    query = parts.query
    query_redacted = False
    if query:
        query, query_redacted = _redact_query(query)
    if not (netloc_redacted or query_redacted):
        return text
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))


def _redact_netloc(netloc: str) -> tuple[str, bool]:
    if "@" not in netloc:
        return netloc, False
    userinfo, _, hostinfo = netloc.rpartition("@")
    if ":" not in userinfo:
        return netloc, False
    user, sep, _ = userinfo.partition(":")
    return f"{user}{sep}{REDACTED_VALUE}@{hostinfo}", True
