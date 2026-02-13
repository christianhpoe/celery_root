# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Celery worker with text-processing demo tasks."""

from __future__ import annotations

import os
import re
import string
from collections import Counter

from .common import create_celery_app

broker_url = os.environ.get("BROKER2_URL") or "amqp://guest:guest@localhost:5673//"  # broker 2
backend_url = os.environ.get("BACKEND2_URL") or "redis://localhost:6380/0"  # backend 2

app = create_celery_app("demo_text", broker_url=broker_url, backend_url=backend_url)

_WORD_RE = re.compile(r"\b\w+\b")
_JSON_MULTITASK_SAMPLE: str = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. "
    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua." * 20
)
_JSON_MULTITASK_PAYLOAD: tuple[str, str, str, str, str] = (
    _JSON_MULTITASK_SAMPLE,
    _JSON_MULTITASK_SAMPLE,
    _JSON_MULTITASK_SAMPLE,
    _JSON_MULTITASK_SAMPLE,
    _JSON_MULTITASK_SAMPLE,
)


class DemoTaskFailureError(RuntimeError):
    """Raised by demo tasks to simulate a failure."""


def _words(text: str) -> list[str]:
    return _WORD_RE.findall(text.lower())


@app.task(name="text.word_count")
def word_count(text: str) -> int:
    """Count words in the given text."""
    return len(_words(text))


@app.task(name="text.sum_counts")
def sum_counts(counts: list[int]) -> int:
    """Sum a list of counts (used for chord callbacks)."""
    return int(sum(counts))


@app.task(name="text.line_count")
def line_count(text: str) -> int:
    """Count lines by splitting on newlines."""
    return text.count("\n") + 1 if text else 0


@app.task(name="text.char_count")
def char_count(text: str, *, include_spaces: bool = True) -> int:
    """Count characters, optionally excluding whitespace."""
    return len(text) if include_spaces else len(text.replace(" ", ""))


@app.task(name="text.unique_word_count")
def unique_word_count(text: str) -> int:
    """Return the number of unique words."""
    return len(set(_words(text)))


@app.task(name="text.most_common_words")
def most_common_words(text: str, top_n: int = 5) -> list[tuple[str, int]]:
    """Return the top N most common words and their counts."""
    counter = Counter(_words(text))
    return counter.most_common(top_n)


@app.task(name="text.longest_word")
def longest_word(text: str) -> str | None:
    """Return the longest word or None if input is empty."""
    words = _words(text)
    return max(words, key=len, default=None)


@app.task(name="text.shortest_word")
def shortest_word(text: str) -> str | None:
    """Return the shortest word or None if input is empty."""
    words = _words(text)
    return min(words, key=len, default=None)


@app.task(name="text.average_word_length")
def average_word_length(text: str) -> float:
    """Return the average word length, treating empty text as zero."""
    words = _words(text)
    if not words:
        return 0.0
    return sum(len(word) for word in words) / len(words)


@app.task(name="text.sentence_count")
def sentence_count(text: str) -> int:
    """Count sentences based on punctuation markers."""
    return len([chunk for chunk in re.split(r"[.!?]+", text) if chunk.strip()])


@app.task(name="text.reverse")
def reverse_text(text: str) -> str:
    """Reverse the full string."""
    return text[::-1]


@app.task(name="text.json")
def json_text(text_1: str, text_2: str, text_3: str, text_4: str, text_5: str) -> dict[str, str]:
    """Return the text as a JSON object."""
    return {"text_1": text_1, "text_2": text_2, "text_3": text_3, "text_4": text_4, "text_5": text_5}


@app.task(name="Schedule.JSON.Multitask")
def schedule_json_multitask(total: int = 50) -> int:
    """Dispatch a batch of JSON tasks for demo scheduling."""
    if total <= 0:
        message = "total must be positive"
        raise ValueError(message)

    payload = _JSON_MULTITASK_PAYLOAD
    for index in range(total):
        if index % 2 == 0:
            json_text.s(*payload).apply_async()
        else:
            json_text.s(
                text_1=payload[0],
                text_2=payload[1],
                text_3=payload[2],
                text_4=payload[3],
                text_5=payload[4],
            ).apply_async()
    return total


@app.task(name="text.fail")
def fail_text() -> None:
    """Fail the task."""
    raise DemoTaskFailureError


@app.task(name="text.uppercase")
def uppercase_text(text: str) -> str:
    """Convert text to uppercase."""
    return text.upper()


@app.task(name="text.lowercase")
def lowercase_text(text: str) -> str:
    """Convert text to lowercase."""
    return text.lower()


@app.task(name="text.title_case")
def title_case_text(text: str) -> str:
    """Convert text to title case."""
    return text.title()


@app.task(name="text.find_substring")
def find_substring_positions(text: str, needle: str) -> list[int]:
    """Return all start indices of needle in text."""
    if not needle:
        return []
    positions: list[int] = []
    start = 0
    while True:
        idx = text.find(needle, start)
        if idx == -1:
            break
        positions.append(idx)
        start = idx + len(needle)
    return positions


@app.task(name="text.replace_substring")
def replace_substring(text: str, old: str, new: str, count: int | None = None) -> str:
    """Replace occurrences of old with new, optionally limiting the count."""
    return text.replace(old, new, count if count is not None else -1)


@app.task(name="text.strip_punctuation")
def strip_punctuation(text: str) -> str:
    """Strip punctuation characters from the text."""
    table = str.maketrans("", "", string.punctuation)
    return text.translate(table)


@app.task(name="text.sort_words")
def sort_words(text: str) -> list[str]:
    """Return words sorted alphabetically."""
    return sorted(_words(text))


@app.task(name="text.deduplicate_words")
def deduplicate_words(text: str) -> list[str]:
    """Return a sorted list of unique words."""
    return sorted(set(_words(text)))


@app.task(name="text.ngrams")
def ngrams(text: str, n: int = 2) -> list[tuple[str, ...]]:
    """Return a list of n-gram tuples from the text."""
    if n <= 0:
        message = "n must be positive"
        raise ValueError(message)
    tokens = _words(text)
    return [tuple(tokens[i : i + n]) for i in range(len(tokens) - n + 1)]


@app.task(name="text.vowel_consonant_counts")
def vowel_consonant_counts(text: str) -> dict[str, int]:
    """Count vowels and consonants separately."""
    vowels = set("aeiou")
    counts = {"vowels": 0, "consonants": 0}
    for char in _words(text):
        for letter in char:
            if letter in vowels:
                counts["vowels"] += 1
            elif letter.isalpha():
                counts["consonants"] += 1
    return counts


@app.task(name="text.palindrome_count")
def palindrome_count(text: str, min_length: int = 3) -> int:
    """Count palindromic words with a minimum length."""
    return sum(1 for word in _words(text) if len(word) >= min_length and word == word[::-1])
