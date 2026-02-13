# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Schedule a fixed number of demo tasks across both worker pools."""

from __future__ import annotations

import logging
from collections.abc import Callable

from celery.canvas import Signature

from demo.worker_math import (
    fibonacci,
    multiply,
    power,
    sum_of_squares,
)
from demo.worker_text import (
    average_word_length,
    char_count,
    most_common_words,
    word_count,
)

type TaskFactory = Callable[[int], Signature]

_TEXT_SAMPLES: tuple[str, ...] = (
    "Celery Root coordinates tasks and workers.",
    "Monitoring queues and results helps keep systems healthy.",
    "Text tasks show how workers can scale across queues.",
    "Math tasks provide some quick compute workloads.",
)

_LOGGER = logging.getLogger(__name__)


def _math_multiply(index: int) -> Signature:
    return multiply.s(index, index + 1)


def _math_power(index: int) -> Signature:
    exponent = index % 6 + 2
    return power.s(2, exponent)


def _math_fibonacci(index: int) -> Signature:
    value = index % 24
    return fibonacci.s(value)


def _math_sum_squares(index: int) -> Signature:
    value = index % 500
    return sum_of_squares.s(value)


def _text_word_count(index: int) -> Signature:
    text = _TEXT_SAMPLES[index % len(_TEXT_SAMPLES)]
    return word_count.s(text)


def _text_char_count(index: int) -> Signature:
    text = _TEXT_SAMPLES[index % len(_TEXT_SAMPLES)]
    include_spaces = index % 2 == 0
    return char_count.s(text, include_spaces=include_spaces)


def _text_common_words(index: int) -> Signature:
    text = _TEXT_SAMPLES[index % len(_TEXT_SAMPLES)]
    top_n = index % 4 + 2
    return most_common_words.s(text, top_n)


def _text_average_length(index: int) -> Signature:
    text = _TEXT_SAMPLES[index % len(_TEXT_SAMPLES)]
    return average_word_length.s(text)


def schedule_tasks(total: int = 100) -> None:
    """Schedule a batch of demo tasks across math and text workers."""
    if total <= 0:
        message = "total must be positive"
        raise ValueError(message)

    math_factories: tuple[TaskFactory, ...] = (
        _math_multiply,
        _math_power,
        _math_fibonacci,
        _math_sum_squares,
    )
    text_factories: tuple[TaskFactory, ...] = (
        _text_word_count,
        _text_char_count,
        _text_common_words,
        _text_average_length,
    )

    for index in range(total):
        if index % 2 == 0:
            factory = math_factories[index % len(math_factories)]
        else:
            factory = text_factories[index % len(text_factories)]
        signature = factory(index)
        signature.apply_async()

    math_count = (total + 1) // 2
    text_count = total // 2
    _LOGGER.info("Scheduled %d demo tasks (%d math, %d text).", total, math_count, text_count)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    schedule_tasks()
