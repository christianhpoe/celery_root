# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Seed demo tasks for the Celery Root stack."""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol, cast

from celery import Celery, chain, chord, group

from demo.common import publish_task_relation
from demo.worker_math import (
    app as math_app,
)
from demo.worker_math import (
    chord_demo,
    chunks_demo,
    collatz_steps,
    estimate_pi,
    factorial,
    fibonacci,
    greatest_common_divisor,
    group_demo,
    logarithm,
    map_demo,
    moving_average,
    multiply,
    nth_root,
    power,
    prime_factors,
    random_walk_distance,
    scale_and_offset,
    stamp_demo,
    starmap_demo,
    sum_list,
    sum_of_squares,
)
from demo.worker_sleep import (
    app as sleep_app,  # noqa: F401
)
from demo.worker_sleep import sleep_then_hello, sleep_until
from demo.worker_text import (
    app as text_app,
)
from demo.worker_text import (
    average_word_length,
    char_count,
    deduplicate_words,
    find_substring_positions,
    line_count,
    lowercase_text,
    most_common_words,
    ngrams,
    palindrome_count,
    sentence_count,
    strip_punctuation,
    sum_counts,
    title_case_text,
    unique_word_count,
    uppercase_text,
    vowel_consonant_counts,
    word_count,
)

PARAGRAPHS: list[str] = [
    "Celery Root keeps an eye on your workers and tasks with ease.",
    "Chains and chords help demonstrate distributed task flows in the demo stack.",
    "Text processing tasks count words, lines, and explore simple analytics.",
]

_LOGGER = logging.getLogger(__name__)


class _ResultLike(Protocol):
    id: str


class _GroupResultLike(Protocol):
    id: str
    results: list[_ResultLike]


class _ChordResultLike(Protocol):
    id: str
    parent: _GroupResultLike | None


def _sample_paragraph(seed: int) -> str:
    """Pick a paragraph deterministically from a seed value."""
    return PARAGRAPHS[seed % len(PARAGRAPHS)]


def seed_math_tasks() -> None:
    """Dispatch math demo tasks."""
    # Simple one-off tasks.
    simple_jobs = [
        multiply.s(3, 7),
        power.s(2, 10),
        factorial.s(8),
        fibonacci.s(22),
        nth_root.s(256, 4),
        prime_factors.s(9_961),
        sum_of_squares.s(500),
        greatest_common_divisor.s(544, 119),
        random_walk_distance.s(2_000, seed=42),
        scale_and_offset.s([1, 2, 3, 4, 5], 1.5, 0.25),
        estimate_pi.s(200_000),
        collatz_steps.s(97_531),
    ]
    for sig in simple_jobs:
        sig.apply_async()

    # Chain: (3 * 4) ^ 2 -> log10.
    chain(multiply.s(3, 4), power.s(2), logarithm.s(base=10)).apply_async()

    # Moving average over a generated series.
    moving_average.s([float(i % 10) for i in range(50)], 5).apply_async()

    # Chord: multiple Fibonacci numbers then sum.
    chord_result = chord([fibonacci.s(n) for n in range(25, 35)])(sum_list.s())
    _publish_chord_relations(math_app, cast("_ChordResultLike", chord_result))

    # Chords: matrix multiplication (dot products per cell).
    left_matrix: list[list[float]] = [
        [1.0, 2.0, 3.0],
        [4.0, 5.0, 6.0],
    ]
    right_matrix: list[list[float]] = [
        [7.0, 8.0],
        [9.0, 10.0],
        [11.0, 12.0],
    ]
    right_columns = list(zip(*right_matrix, strict=True))
    for row in left_matrix:
        for column in right_columns:
            chord_result = chord([multiply.s(a, b) for a, b in zip(row, column, strict=True)])(sum_list.s())
            _publish_chord_relations(math_app, cast("_ChordResultLike", chord_result))

    # Group: explore multiple roots.
    group(nth_root.s(10_000, n) for n in range(2, 12)).apply_async()

    # Demo roots for graph UI flows.
    group_demo.s().apply_async()
    chord_demo.s().apply_async()
    map_demo.s().apply_async()
    starmap_demo.s().apply_async()
    chunks_demo.s().apply_async()
    cast("Any", stamp_demo.s()).stamp(stamp="root-stamp", flow="stamp-root").apply_async()


def seed_text_tasks() -> None:
    """Dispatch text demo tasks."""
    sentences = [_sample_paragraph(seed) for seed in range(10)]

    for text in sentences:
        word_count.s(text).apply_async()
        line_count.s(text).apply_async()
        char_count.s(text).apply_async()
        unique_word_count.s(text).apply_async()
        most_common_words.s(text, 3).apply_async()
        average_word_length.s(text).apply_async()
        sentence_count.s(text).apply_async()
        uppercase_text.s(text).apply_async()
        lowercase_text.s(text).apply_async()
        title_case_text.s(text).apply_async()
        strip_punctuation.s(text).apply_async()
        deduplicate_words.s(text).apply_async()
        ngrams.s(text, 2).apply_async()
        vowel_consonant_counts.s(text).apply_async()
        palindrome_count.s(text, 3).apply_async()

    # String search and grouping demonstrations.
    group(find_substring_positions.s(paragraph, "task") for paragraph in PARAGRAPHS).apply_async()

    chord_result = chord([word_count.s(paragraph) for paragraph in PARAGRAPHS])(sum_counts.s())
    _publish_chord_relations(text_app, cast("_ChordResultLike", chord_result))


def seed_sleep_tasks() -> None:
    """Dispatch sleep demo tasks."""
    for _ in range(5):
        sleep_then_hello.s().apply_async()

    now = time.time()
    sleep_until.delay(target_timestamp=now + 100)


def seed() -> None:
    """Dispatch all demo tasks."""
    seed_math_tasks()
    seed_text_tasks()
    seed_sleep_tasks()
    _LOGGER.info("Dispatched demo tasks to RabbitMQ/Redis brokers.")


def _publish_chord_relations(app: Celery, result: _ChordResultLike) -> None:
    group_result = result.parent
    group_id = group_result.id if group_result else None
    callback_id = result.id
    if not group_id or not callback_id:
        return
    children = group_result.results if group_result else []
    for child in children:
        child_id = child.id
        publish_task_relation(
            app=app,
            root_id=callback_id,
            parent_id=group_id,
            child_id=child_id,
            relation="chord",
        )
    publish_task_relation(
        app=app,
        root_id=callback_id,
        parent_id=group_id,
        child_id=callback_id,
        relation="chord",
    )


if __name__ == "__main__":
    seed()
