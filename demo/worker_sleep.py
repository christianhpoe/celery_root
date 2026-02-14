# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Celery worker with a random sleep task."""

from __future__ import annotations

import os
import random
import time
from typing import TYPE_CHECKING, Any

from .common import create_celery_app

if TYPE_CHECKING:
    from celery import Task

broker_url = os.environ.get("BROKER3_URL") or "redis://localhost:6381/0"
backend_url = os.environ.get("BACKEND3_URL") or "db+postgresql://postgres:postgres@localhost:5432/postgres"

app = create_celery_app("demo_sleep", queue="sleep", broker_url=broker_url, backend_url=backend_url)


@app.task(name="sleep.hello", rate_limit="1/m")
def sleep_then_hello(min_seconds: float = 0.5, max_seconds: float = 2.0) -> str:
    """Sleep for a random duration and return hello world."""
    if min_seconds < 0 or max_seconds < 0:
        message = "sleep bounds must be non-negative"
        raise ValueError(message)
    if max_seconds < min_seconds:
        message = "max_seconds must be >= min_seconds"
        raise ValueError(message)
    duration = random.uniform(min_seconds, max_seconds)  # noqa: S311
    time.sleep(duration)
    return "hello world"


@app.task(name="sleep.sleep_until")
def sleep_until(target_timestamp: float) -> None:
    """Sleep for a random duration and return hello world."""
    now = time.time()
    if now > target_timestamp:
        sleep_then_hello.delay()
    else:
        sleep_until.delay(target_timestamp=target_timestamp)


@app.task(name="sleep.fail_randomly")
def sleep_fail_randomly(min_seconds: float = 0.5, max_seconds: float = 2.0, failure_rate: float = 0.3) -> str:
    """Sleep for a random duration and occasionally fail."""
    if min_seconds < 0 or max_seconds < 0:
        message = "sleep bounds must be non-negative"
        raise ValueError(message)
    if max_seconds < min_seconds:
        message = "max_seconds must be >= min_seconds"
        raise ValueError(message)
    if not 0.0 <= failure_rate <= 1.0:
        message = "failure_rate must be between 0 and 1"
        raise ValueError(message)
    duration = random.uniform(min_seconds, max_seconds)  # noqa: S311
    time.sleep(duration)
    if random.random() < failure_rate:  # noqa: S311
        msg = "Random sleep failure"
        raise RuntimeError(msg)
    return "ok"


@app.task(name="sleep.retry_randomly", bind=True, max_retries=3)
def sleep_retry_randomly(
    self: Task[Any, Any],
    min_seconds: float = 0.5,
    max_seconds: float = 2.0,
    retry_rate: float = 0.3,
) -> str:
    """Sleep for a random duration and occasionally retry."""
    if min_seconds < 0 or max_seconds < 0:
        message = "sleep bounds must be non-negative"
        raise ValueError(message)
    if max_seconds < min_seconds:
        message = "max_seconds must be >= min_seconds"
        raise ValueError(message)
    if not 0.0 <= retry_rate <= 1.0:
        message = "retry_rate must be between 0 and 1"
        raise ValueError(message)
    duration = random.uniform(min_seconds, max_seconds)  # noqa: S311
    time.sleep(duration)
    if random.random() < retry_rate:  # noqa: S311
        countdown = random.uniform(0.5, 2.0)  # noqa: S311
        raise self.retry(exc=RuntimeError("Random sleep retry"), countdown=countdown)
    return "ok"
