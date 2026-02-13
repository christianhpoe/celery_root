# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Template context processors for component metadata."""

from __future__ import annotations

from .components import component_snapshot


def component_context(_: object) -> dict[str, object]:
    """Inject component metadata into template context."""
    snapshot = component_snapshot()
    nav = {key: info.enabled for key, info in snapshot.items()}
    return {"component_snapshot": snapshot, "component_nav": nav}
