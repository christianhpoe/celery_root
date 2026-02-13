# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Documentation pages for the web UI."""

from __future__ import annotations

from typing import TYPE_CHECKING

from django.shortcuts import render

if TYPE_CHECKING:
    from django.http import HttpRequest, HttpResponse


def observability(request: HttpRequest) -> HttpResponse:
    """Render the observability documentation page."""
    return render(
        request,
        "docs/observability.html",
        {
            "title": "Observability",
        },
    )
