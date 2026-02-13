# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Custom error handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.shortcuts import render

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def handler404(request: HttpRequest, exception: Exception) -> HttpResponse:  # noqa: ARG001
    """Render a friendly 404 page."""
    return render(
        request,
        "404.html",
        {
            "title": "Page not found",
            "path": request.path,
        },
        status=404,
    )
