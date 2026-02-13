# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Log viewer pages for the web UI."""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render

from celery_root.core.logging.utils import LOG_FILE_PREFIX

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse

_DEFAULT_LINES = 500
_MAX_LINES = 5000
_CHUNK_SIZE = 8192
_MAX_BYTES = 1024 * 1024
_LOG_PREFIX = f"{LOG_FILE_PREFIX}-"
_LOG_SUFFIX = ".log"


def _log_dir() -> Path:
    raw = getattr(settings, "CELERY_ROOT_LOG_DIR", None)
    return Path(raw) if raw is not None else Path("logs")


def _list_log_files(log_dir: Path) -> list[Path]:
    if not log_dir.exists() or not log_dir.is_dir():
        return []
    files = [path for path in log_dir.glob(f"{LOG_FILE_PREFIX}*.log*") if path.is_file()]
    files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return files


def _component_from_filename(name: str) -> str | None:
    if name == f"{LOG_FILE_PREFIX}.log":
        return "default"
    if not name.startswith(_LOG_PREFIX):
        return None
    remainder = name[len(_LOG_PREFIX) :]
    if _LOG_SUFFIX not in remainder:
        return None
    return remainder.split(_LOG_SUFFIX, 1)[0]


def _group_log_files(files: list[Path]) -> dict[str, list[Path]]:
    grouped: dict[str, list[Path]] = {}
    for path in files:
        component = _component_from_filename(path.name)
        if component is None:
            continue
        grouped.setdefault(component, []).append(path)
    for component_files in grouped.values():
        component_files.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return grouped


def _read_tail(path: Path, lines: int) -> str:
    if lines <= 0:
        return ""
    lines = min(lines, _MAX_LINES)
    data = b""
    with path.open("rb") as handle:
        handle.seek(0, os.SEEK_END)
        cursor = handle.tell()
        total = 0
        while cursor > 0 and data.count(b"\n") <= lines and total < _MAX_BYTES:
            read_size = min(_CHUNK_SIZE, cursor)
            cursor -= read_size
            handle.seek(cursor)
            chunk = handle.read(read_size)
            data = chunk + data
            total += read_size
    text = data.decode("utf-8", errors="replace")
    return "\n".join(text.splitlines()[-lines:])


def logs(request: HttpRequest) -> HttpResponse:
    """Render the logs page."""
    log_dir = _log_dir()
    files = _list_log_files(log_dir)
    grouped_files = _group_log_files(files)
    components = sorted(grouped_files.keys())
    selected_file = request.GET.get("file", "")
    selected_component = request.GET.get("component") or _component_from_filename(selected_file)
    if not selected_component and components:
        selected_component = components[0]
    try:
        lines = int(request.GET.get("lines", str(_DEFAULT_LINES)))
    except ValueError:
        lines = _DEFAULT_LINES
    lines = max(1, min(lines, _MAX_LINES))

    content = ""
    error = None
    selected_path = None

    if selected_component:
        component_files = grouped_files.get(selected_component, [])
        if selected_file:
            selected_path = next((path for path in component_files if path.name == selected_file), None)
        if selected_path is None and component_files:
            selected_path = component_files[0]
        if selected_path is None:
            error = "Selected log component was not found."
        else:
            try:
                content = _read_tail(selected_path, lines)
            except OSError as exc:
                error = f"Failed to read log file: {exc}"

    context = {
        "title": "Logs",
        "components": components,
        "selected_component": selected_component or "",
        "selected_file": selected_path.name if selected_path is not None else "",
        "log_dir": str(log_dir),
        "lines": lines,
        "content": content,
        "error": error,
    }
    wants_json = request.GET.get("format") == "json" or "application/json" in request.headers.get("accept", "")
    if wants_json:
        return JsonResponse(
            {
                "selected_component": context["selected_component"],
                "selected_file": context["selected_file"],
                "lines": context["lines"],
                "content": context["content"],
                "error": context["error"],
            },
        )
    return render(request, "logs.html", context)
