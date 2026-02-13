# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from celery import Celery

app = Celery(
    "fixture_one",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)


@app.task(name="fixture.add")
def add(a: int, b: int) -> int:
    return a + b
