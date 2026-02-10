"""Celery worker with compute-heavy demo tasks."""

from __future__ import annotations

import math
import os
import time
from secrets import randbelow
from typing import TYPE_CHECKING, Any, Protocol, cast
from uuid import uuid4

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from celery import Task

from celery import chord, group

from .common import create_celery_app, publish_task_relation


class _ResultLike(Protocol):
    id: str


class _GroupResultLike(Protocol):
    id: str
    results: list[_ResultLike]


class MissingTaskIdError(RuntimeError):
    """Raised when a task id is unexpectedly missing."""

    def __init__(self, context: str) -> None:
        """Build an error for missing task ids in demo tasks."""
        message = f"Task id missing for {context}"
        super().__init__(message)


def _require_task_id(task_id: str | None, context: str) -> str:
    if task_id is None:
        raise MissingTaskIdError(context)
    return task_id


broker_url = os.environ.get("BROKER_URL") or "amqp://guest:guest@localhost:5672//"  # broker 1
backend_url = os.environ.get("BACKEND_URL") or "redis://localhost:6379/0"  # backend 1

app = create_celery_app("demo_math", broker_url=broker_url, backend_url=backend_url)


def _link_root_to_canvas(root_id: str, canvas_id: str) -> None:
    publish_task_relation(
        app=app,
        root_id=root_id,
        parent_id=root_id,
        child_id=canvas_id,
        relation="chain",
    )


def _link_canvas_children(root_id: str, canvas_id: str, child_ids: list[str], relation: str) -> None:
    for child_id in child_ids:
        publish_task_relation(
            app=app,
            root_id=root_id,
            parent_id=canvas_id,
            child_id=child_id,
            relation=relation,
        )


@app.task(name="math.multiply")
def multiply(x: float, y: float) -> float:
    """Multiply two numbers after a brief delay."""
    time.sleep(0.05)
    return x * y


@app.task(name="math.power")
def power(base: float, exponent: float) -> float:
    """Raise base to exponent."""
    return base**exponent


@app.task(name="math.factorial")
def factorial(n: int) -> int:
    """Compute n! using an iterative loop."""
    result = 1
    for i in range(2, n + 1):
        result *= i
    return result


@app.task(name="math.fibonacci")
def fibonacci(n: int) -> int:
    """Return the nth Fibonacci number (0-indexed)."""
    a, b = 0, 1
    time.sleep(randbelow(11) + 5)
    for _ in range(n):
        a, b = b, a + b
    return a


@app.task(name="math.logarithm")
def logarithm(value: float, base: float = math.e) -> float:
    """Compute the logarithm of value with the given base."""
    return math.log(value, base)


@app.task(name="math.nth_root")
def nth_root(value: float, n: int) -> float:
    """Compute the nth root of value using exponentiation."""
    return value ** (1 / n)


@app.task(name="math.prime_factors")
def prime_factors(n: int) -> list[int]:
    """Return the prime factors of n."""
    factors: list[int] = []
    candidate = 2
    remainder = n
    while candidate * candidate <= remainder:
        while remainder % candidate == 0:
            factors.append(candidate)
            remainder //= candidate
        candidate += 1
    if remainder > 1:
        factors.append(remainder)
    return factors


@app.task(name="math.gcd")
def greatest_common_divisor(a: int, b: int) -> int:
    """Compute the greatest common divisor using Euclid's algorithm."""
    while b:
        a, b = b, a % b
    return abs(a)


@app.task(name="math.untyped_difference")
def untyped_difference(a: Any, b: Any) -> Any:  # noqa: ANN401
    """Subtract values without specific type hints (UI warning demo)."""
    return a - b


@app.task(name="math.lcm")
def least_common_multiple(a: int, b: int) -> int:
    """Compute the least common multiple."""
    gcd_value = greatest_common_divisor(a, b)
    return abs(a * b) // gcd_value if gcd_value else 0


@app.task(name="math.sum_range")
def sum_range(start: int, end: int) -> int:
    """Sum integers from start to end inclusive."""
    if start > end:
        start, end = end, start
    return sum(range(start, end + 1))


@app.task(name="math.sum_of_squares")
def sum_of_squares(limit: int) -> int:
    """Return the sum of squares up to limit."""
    return sum(i * i for i in range(limit + 1))


@app.task(name="math.harmonic_mean")
def harmonic_mean(values: Sequence[float]) -> float:
    """Compute the harmonic mean of a sequence of numbers."""
    if not values:
        message = "values must not be empty"
        raise ValueError(message)
    return len(values) / sum(1 / v for v in values)


@app.task(name="math.geometric_mean")
def geometric_mean(values: Sequence[float]) -> float:
    """Compute the geometric mean of a sequence of numbers."""
    if not values:
        message = "values must not be empty"
        raise ValueError(message)
    product = 1.0
    for value in values:
        product *= value
    return product ** (1 / len(values))


@app.task(name="math.estimate_pi")
def estimate_pi(iterations: int = 1_000_000) -> float:
    """Estimate pi using the Leibniz series for a fixed number of iterations."""
    acc = 0.0
    sign = 1.0
    for i in range(iterations):
        acc += sign / (2 * i + 1)
        sign *= -1.0
    return 4 * acc


@app.task(name="math.collatz_steps")
def collatz_steps(n: int) -> int:
    """Return the number of steps to reach 1 via the Collatz sequence."""
    steps = 0
    value = n
    while value != 1:
        value = value // 2 if value % 2 == 0 else 3 * value + 1
        steps += 1
    return steps


@app.task(name="math.random_walk_distance")
def random_walk_distance(steps: int, seed: int | None = None) -> float:
    """Simulate a 2D random walk and return the final distance from origin."""
    state = seed if seed is not None else int(time.time_ns())
    directions = ((1, 0), (-1, 0), (0, 1), (0, -1))
    x, y = 0, 0
    for _ in range(steps):
        state = (1103515245 * state + 12345) % (2**31)
        direction = state % 4
        dx, dy = directions[direction]
        x += dx
        y += dy
    return math.hypot(x, y)


@app.task(name="math.dot_product")
def dot_product(left: Sequence[float], right: Sequence[float]) -> float:
    """Compute the dot product of two equally sized sequences."""
    if len(left) != len(right):
        message = "Sequences must be the same length"
        raise ValueError(message)
    return sum(a * b for a, b in zip(left, right, strict=True))


@app.task(name="math.moving_average")
def moving_average(values: Sequence[float], window: int) -> list[float]:
    """Compute a simple moving average across a sequence."""
    if window <= 0:
        message = "window must be positive"
        raise ValueError(message)
    result: list[float] = []
    for i in range(len(values) - window + 1):
        chunk = values[i : i + window]
        result.append(sum(chunk) / window)
    return result


@app.task(name="math.trapezoidal_integral")
def trapezoidal_integral(y_values: Sequence[float], step: float) -> float:
    """Approximate the integral for evenly spaced samples using the trapezoidal rule."""
    min_samples = 2
    if step <= 0:
        message = "step must be positive"
        raise ValueError(message)
    if len(y_values) < min_samples:
        message = "y_values must contain at least two samples"
        raise ValueError(message)
    total = sum(y_values) - (y_values[0] + y_values[-1]) / 2
    return total * step


@app.task(name="math.scale_and_offset")
def scale_and_offset(values: Iterable[float], scale: float, offset: float) -> list[float]:
    """Apply a linear transform to a sequence of values."""
    return [scale * value + offset for value in values]


@app.task(name="math.sum_list")
def sum_list(values: Sequence[float]) -> float:
    """Sum a sequence of numbers, returning a float for chord compatibility."""
    return float(sum(values))


@app.task(bind=True, name="math.group")
def group_demo(self: Task[Any, Any]) -> str:
    """Create a group fan-out and relate it back to this root task."""
    result = group(sum_range.s(i * 10, i * 10 + 9) for i in range(6)).apply_async()
    root_id = _require_task_id(self.request.id, "math.group demo")
    group_result = cast("_GroupResultLike", result)
    child_ids = [child.id for child in group_result.results]
    _link_canvas_children(root_id, root_id, child_ids, "group")
    return root_id


@app.task(bind=True, name="math.chord")
def chord_demo(self: Task[Any, Any]) -> str:
    """Create a chord fan-out and relate it back to this root task."""
    time.sleep(15)
    header = [fibonacci.s(n) for n in range(12, 18)]
    result = chord(header)(sum_list.s())
    root_id = _require_task_id(self.request.id, "math.chord demo")
    group_result = cast("_GroupResultLike | None", result.parent)
    canvas_id = group_result.id if group_result else uuid4().hex
    _link_root_to_canvas(root_id, canvas_id)
    child_ids = [child.id for child in (group_result.results if group_result else [])]
    _link_canvas_children(root_id, canvas_id, child_ids, "chord")
    callback_id = result.id
    if callback_id is not None:
        _link_canvas_children(root_id, canvas_id, [_require_task_id(callback_id, "math.chord callback")], "chord")
    return canvas_id


@app.task(bind=True, name="math.map")
def map_demo(self: Task[Any, Any]) -> str:
    """Create a map-style fan-out and relate it back to this root task."""
    root_id = _require_task_id(self.request.id, "math.map demo")
    child_ids = [_require_task_id(fibonacci.s(n).apply_async().id, "math.map child") for n in range(18, 24)]
    _link_canvas_children(root_id, root_id, child_ids, "map")
    return root_id


@app.task(bind=True, name="math.starmap")
def starmap_demo(self: Task[Any, Any]) -> str:
    """Create a starmap-style fan-out and relate it back to this root task."""
    pairs = [(2, 3), (3, 5), (5, 8), (8, 13), (13, 21)]
    root_id = _require_task_id(self.request.id, "math.starmap demo")
    child_ids = [_require_task_id(multiply.s(a, b).apply_async().id, "math.starmap child") for a, b in pairs]
    _link_canvas_children(root_id, root_id, child_ids, "starmap")
    return root_id


@app.task(bind=True, name="math.chunks")
def chunks_demo(self: Task[Any, Any]) -> str:
    """Create a chunks-style fan-out and relate it back to this root task."""
    chunks = [list(range(i, i + 5)) for i in range(0, 20, 5)]
    root_id = _require_task_id(self.request.id, "math.chunks demo")
    child_ids = [_require_task_id(sum_list.s(chunk).apply_async().id, "math.chunks child") for chunk in chunks]
    _link_canvas_children(root_id, root_id, child_ids, "chunks")
    return root_id


@app.task(bind=True, name="math.stamp")
def stamp_demo(self: Task[Any, Any]) -> str:
    """Run a stamped task so the UI can show stamps."""
    stamped = cast("Any", multiply.s(7, 9)).stamp(stamp="math-demo", flow="stamp")
    result = stamped.apply_async()
    root_id = _require_task_id(self.request.id, "math.stamp demo")
    child_id = _require_task_id(result.id, "math.stamp child")
    publish_task_relation(
        app=app,
        root_id=root_id,
        parent_id=root_id,
        child_id=child_id,
        relation="chain",
    )
    return child_id


def _raise_math_failure(kind: str) -> None:
    if kind == "zero_division":
        _divide_by_zero()
    if kind == "domain":
        _domain_error()
    raise UnknownDemoFailureError(kind)


class UnknownDemoFailureError(RuntimeError):
    """Raised for unsupported demo failure kinds."""

    def __init__(self, kind: str) -> None:
        """Build an error for an unsupported demo failure kind."""
        message = f"Unknown demo failure kind: {kind}"
        super().__init__(message)


def _divide_by_zero() -> float:
    return 1.0 / 0.0


def _domain_error() -> float:
    return math.log(-1.0)


@app.task(name="math.divide_by_zero")
def divide_by_zero() -> None:
    """Always fail with a ZeroDivisionError for traceback demos."""
    _raise_math_failure("zero_division")


@app.task(name="math.domain_error")
def domain_error() -> None:
    """Always fail with a ValueError for traceback demos."""
    _raise_math_failure("domain")
