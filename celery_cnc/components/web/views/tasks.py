"""Task list and detail views."""

from __future__ import annotations

import ast
import inspect
import json
import math
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from functools import cmp_to_key
from types import UnionType
from typing import TYPE_CHECKING, Annotated, Any, Literal, TypedDict, Union, cast, get_args, get_origin
from urllib.parse import quote_plus

from django.http import Http404, HttpResponseBadRequest
from django.shortcuts import redirect, render
from django.utils import timezone

from celery_cnc.components.web.services import app_name, get_registry, list_task_names, open_db
from celery_cnc.core.db.models import Task, TaskFilter, TimeRange
from celery_cnc.core.engine import tasks as task_control

from .decorators import require_post

if TYPE_CHECKING:
    from celery import Celery
    from django.http import HttpRequest, HttpResponse, QueryDict

    from celery_cnc.core.db.adapters.base import BaseDBController
    from celery_cnc.core.db.models import TaskRelation

_TASK_NOT_FOUND = "Task not found"
_TASK_NAME_REQUIRED = "Task name is required"

_FINAL_STATES = {"SUCCESS", "FAILURE", "REVOKED"}

STATE_BADGES = {
    "SUCCESS": "badge-success",
    "FAILURE": "badge-danger",
    "STARTED": "badge-warning",
    "RETRY": "badge-warning",
    "PENDING": "badge-info",
    "RECEIVED": "badge-info",
    "REVOKED": "badge-muted",
}

STATE_OPTIONS: Sequence[tuple[str, str]] = (
    ("PENDING", "Pending"),
    ("RECEIVED", "Received"),
    ("STARTED", "Started"),
    ("SUCCESS", "Success"),
    ("FAILURE", "Failure"),
    ("RETRY", "Retry"),
    ("REVOKED", "Revoked"),
)

_PAGE_SIZE_OPTIONS: tuple[int, ...] = (10, 25, 50, 100, 200)
_DEFAULT_PAGE_SIZE = 50
_SORTABLE_FIELDS: dict[str, str] = {
    "state": "State",
    "worker": "Worker",
    "received": "Received",
    "started": "Started",
    "runtime": "Runtime",
}
_SORT_DIRECTIONS = ("asc", "desc")
_STATS_SORT_FIELDS: dict[str, str] = {
    "name": "Task",
    "count": "Count",
    "avg": "Avg runtime",
    "p95": "P95",
    "p99": "P99",
    "min": "Min",
    "max": "Max",
}


class _TaskEntry(TypedDict):
    task_id: str
    name: str
    state: str
    badge_class: str
    worker: str
    child_count: int
    runtime: float | None
    received: datetime | None
    started: datetime | None
    finished: datetime | None
    done: bool
    timestamp: datetime | None
    args: str
    kwargs: str
    retries: int | None
    result: str | None
    traceback: str | None
    parent_id: str | None
    root_id: str | None
    group_id: str | None
    chord_id: str | None
    eta: str | None


class _TaskView(TypedDict):
    task_id: str
    name: str
    state: str
    badge_class: str
    worker: str
    child_count: int
    runtime: float | None
    received: datetime | None
    started: datetime | None
    finished: datetime | None
    done: bool
    timestamp: datetime | None
    args: str
    kwargs: str
    retries: int | None
    result: str | None
    traceback: str | None
    parent_id: str | None
    root_id: str | None
    group_id: str | None
    chord_id: str | None
    eta: str | None


class _SortHeader(TypedDict):
    key: str
    label: str
    url: str
    active: bool
    direction: str


class _TaskLink(TypedDict):
    id: str
    label: str
    state: str | None
    badge_class: str
    exists: bool


TaskEntry = _TaskEntry
TaskView = _TaskView


class _AnnotationInfo(TypedDict):
    label: str
    input: str
    options: list[object]


class _TaskParam(TypedDict):
    name: str
    kind: str
    required: bool
    annotation: str
    input: str
    default: str | None
    options: list[object]


class _TaskSchema(TypedDict):
    params: list[_TaskParam]
    doc: str | None
    queue: str | None


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if timezone.is_naive(parsed):
        parsed = timezone.make_aware(parsed)
    return parsed


def _is_date_only(value: str | None) -> bool:
    if not value:
        return False
    return "T" not in value and " " not in value


def _task_timestamp(task: Task) -> datetime | None:
    return task.finished or task.started or task.received


def _task_to_view(task: Task) -> _TaskView:
    timestamp = _task_timestamp(task)
    state = task.state
    return {
        "task_id": task.task_id,
        "name": task.name or task.task_id,
        "state": state,
        "badge_class": STATE_BADGES.get(state, "badge-muted"),
        "worker": task.worker or "—",
        "child_count": 0,
        "runtime": float(task.runtime) if task.runtime is not None else None,
        "received": task.received,
        "started": task.started,
        "finished": task.finished,
        "done": state in _FINAL_STATES,
        "timestamp": timestamp,
        "args": task.args or "",
        "kwargs": task.kwargs or "",
        "retries": task.retries,
        "result": task.result,
        "traceback": task.traceback,
        "parent_id": task.parent_id,
        "root_id": task.root_id,
        "group_id": task.group_id,
        "chord_id": task.chord_id,
        "eta": None,
    }


def _parse_args(value: str) -> list[object]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return []
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, tuple):
        return list(parsed)
    return []


def _parse_kwargs(value: str) -> dict[str, object]:
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(value)
        except (SyntaxError, ValueError):
            return {}
    if isinstance(parsed, dict):
        return dict(parsed)
    return {}


def _strip_ellipsis(value: object) -> object:
    if value is Ellipsis:
        return None
    if isinstance(value, list):
        return [_strip_ellipsis(item) for item in value]
    if isinstance(value, tuple):
        return [_strip_ellipsis(item) for item in value]
    if isinstance(value, dict):
        return {key: _strip_ellipsis(item) for key, item in value.items()}
    return value


_KIND_LABELS: dict[inspect._ParameterKind, str] = {
    inspect.Parameter.POSITIONAL_ONLY: "positional_only",
    inspect.Parameter.POSITIONAL_OR_KEYWORD: "positional_or_keyword",
    inspect.Parameter.KEYWORD_ONLY: "keyword_only",
    inspect.Parameter.VAR_POSITIONAL: "var_positional",
    inspect.Parameter.VAR_KEYWORD: "var_keyword",
}

_STRING_SCALAR_INPUTS: dict[str, str] = {
    "bool": "bool",
    "int": "int",
    "float": "float",
    "str": "str",
}
_STRING_LIST_INPUTS = {
    "collection",
    "iterable",
    "list",
    "mutablesequence",
    "sequence",
    "set",
    "tuple",
    "frozenset",
}
_STRING_DICT_INPUTS = {
    "dict",
    "mapping",
    "mutablemapping",
}
_SIMPLE_ANNOTATIONS: dict[object, tuple[str, str]] = {
    bool: ("bool", "bool"),
    int: ("int", "int"),
    float: ("float", "float"),
    str: ("str", "str"),
    Any: ("any", "text"),
}


def _split_annotation_args(value: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for index, char in enumerate(value):
        if char == "[":
            depth += 1
        elif char == "]":
            depth = max(depth - 1, 0)
        elif char == "," and depth == 0:
            part = value[start:index].strip()
            if part:
                parts.append(part)
            start = index + 1
    tail = value[start:].strip()
    if tail:
        parts.append(tail)
    return parts


def _strip_annotation_prefix(value: str) -> str:
    normalized = value.strip()
    for prefix in ("typing.", "collections.abc.", "collections."):
        if normalized.startswith(prefix):
            return normalized[len(prefix) :]
    return normalized


def _normalize_annotation_text(value: str) -> str:
    normalized = _strip_annotation_prefix(value.strip())
    if normalized.startswith("Annotated[") and normalized.endswith("]"):
        inner = normalized[len("Annotated[") : -1]
        parts = _split_annotation_args(inner)
        if parts:
            normalized = parts[0]
    normalized = _strip_annotation_prefix(normalized.strip())
    if normalized.startswith("Optional[") and normalized.endswith("]"):
        normalized = normalized[len("Optional[") : -1]
    normalized = _strip_annotation_prefix(normalized.strip())
    if normalized.startswith("Union[") and normalized.endswith("]"):
        inner = normalized[len("Union[") : -1]
        parts = _split_annotation_args(inner)
        non_none = [part for part in parts if part not in {"None", "NoneType"}]
        if len(non_none) == 1:
            normalized = non_none[0]
    normalized = _strip_annotation_prefix(normalized.strip())
    if "|" in normalized and "None" in normalized:
        parts = [part.strip() for part in normalized.split("|")]
        non_none = [part for part in parts if part not in {"None", "NoneType"}]
        if len(non_none) == 1:
            normalized = non_none[0]
    return _strip_annotation_prefix(normalized.strip())


def _parse_literal_options(value: str) -> list[object]:
    inner = value[len("Literal[") : -1].strip()
    if not inner:
        return []
    try:
        parsed = ast.literal_eval(f"({inner})")
    except (SyntaxError, ValueError):
        return []
    if not isinstance(parsed, tuple):
        parsed = (parsed,)
    options: list[object] = []
    for item in parsed:
        if isinstance(item, (str, int, float, bool)) or item is None:
            options.append(item)
        else:
            options.append(str(item))
    return options


def _annotation_info_from_text(annotation: str) -> _AnnotationInfo:
    label = _annotation_label(annotation)
    normalized = _normalize_annotation_text(annotation)
    base = normalized.split("[", 1)[0].strip()
    base_lower = base.lower()
    if base_lower in _STRING_SCALAR_INPUTS:
        return {"label": label, "input": _STRING_SCALAR_INPUTS[base_lower], "options": []}
    if base_lower in _STRING_LIST_INPUTS:
        return {"label": label, "input": "json-list", "options": []}
    if base_lower in _STRING_DICT_INPUTS:
        return {"label": label, "input": "json-dict", "options": []}
    if base_lower == "literal" and normalized.endswith("]"):
        options = _parse_literal_options(normalized)
        if options:
            return {"label": label, "input": "select", "options": options}
    return {"label": label, "input": "text", "options": []}


def _literal_options_from_args(args: Sequence[object]) -> list[object]:
    options: list[object] = []
    for item in args:
        if isinstance(item, (str, int, float, bool)) or item is None:
            options.append(item)
        else:
            options.append(str(item))
    return options


def _is_sequence_type(value: object) -> bool:
    if value in {list, tuple, set, frozenset}:
        return True
    if isinstance(value, type):
        return issubclass(value, Sequence) and value is not str
    return False


def _is_mapping_type(value: object) -> bool:
    if value is dict:
        return True
    if isinstance(value, type):
        return issubclass(value, Mapping)
    return False


def _annotation_info_from_object(annotation: object) -> _AnnotationInfo:
    if annotation in _SIMPLE_ANNOTATIONS:
        label, input_type = _SIMPLE_ANNOTATIONS[annotation]
        return {"label": label, "input": input_type, "options": []}
    label = _annotation_label(annotation)
    origin = get_origin(annotation)
    candidate = origin if origin is not None else annotation
    if _is_sequence_type(candidate):
        return {"label": label, "input": "json-list", "options": []}
    if _is_mapping_type(candidate):
        return {"label": label, "input": "json-dict", "options": []}
    if origin is Literal:
        return {"label": label, "input": "select", "options": _literal_options_from_args(get_args(annotation))}
    return {"label": label, "input": "text", "options": []}


def _unwrap_annotated(annotation: object) -> object:
    origin = get_origin(annotation)
    if origin is Annotated:
        args = get_args(annotation)
        if args:
            return args[0]
    return annotation


def _strip_optional(annotation: object) -> tuple[object, bool]:
    origin = get_origin(annotation)
    if origin in (Union, UnionType):
        args = get_args(annotation)
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1:
            return non_none[0], True
    return annotation, False


def _annotation_label(annotation: object) -> str:  # noqa: PLR0911
    if annotation is inspect.Signature.empty:
        return "unknown"
    annotation = _unwrap_annotated(annotation)
    annotation, _optional = _strip_optional(annotation)
    if annotation is Any:
        return "any"
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is None:
        if isinstance(annotation, type):
            return annotation.__name__
        return str(annotation)
    if origin in (list, tuple, set):
        inner = _annotation_label(args[0]) if args else "object"
        return f"{origin.__name__}[{inner}]"
    if origin is dict:
        key_label = _annotation_label(args[0]) if len(args) > 0 else "object"
        value_label = _annotation_label(args[1]) if len(args) > 1 else "object"
        return f"dict[{key_label}, {value_label}]"
    if origin is Literal:
        rendered = ", ".join(str(item) for item in args)
        return f"Literal[{rendered}]"
    if origin in (Union, UnionType):
        rendered = ", ".join(_annotation_label(item) for item in args)
        return f"Union[{rendered}]"
    return str(annotation)


def _annotation_info(annotation: object) -> _AnnotationInfo:
    if annotation is inspect.Signature.empty:
        return {"label": "unknown", "input": "text", "options": []}
    annotation = _unwrap_annotated(annotation)
    annotation, _optional = _strip_optional(annotation)
    if isinstance(annotation, str):
        return _annotation_info_from_text(annotation)
    return _annotation_info_from_object(annotation)


def _task_callable(task: object) -> Callable[..., object] | object:
    target = getattr(task, "run", None)
    if target is None:
        target = task
    if not callable(target):
        return target
    callable_target = cast("Callable[..., object]", target)
    try:
        unwrapped = inspect.unwrap(callable_target)
        return cast("Callable[..., object]", unwrapped)
    except Exception:  # noqa: BLE001 - unwrap is best-effort
        return callable_target


def _task_signature(task: object) -> inspect.Signature | None:
    target = _task_callable(task)
    if not callable(target):
        return None
    try:
        return inspect.signature(target)
    except (TypeError, ValueError):
        return None


def _task_params(task: object, signature: inspect.Signature) -> list[_TaskParam]:
    params = list(signature.parameters.values())
    if params:
        first = params[0]
        if (
            first.name in {"self", "task"}
            and first.kind
            in {
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            }
            and bool(getattr(task, "bind", False))
        ):
            params = params[1:]

    rendered: list[_TaskParam] = []
    for param in params:
        info = _annotation_info(param.annotation)
        required = param.default is inspect.Signature.empty and param.kind not in {
            inspect.Parameter.VAR_POSITIONAL,
            inspect.Parameter.VAR_KEYWORD,
        }
        default = None if param.default is inspect.Signature.empty else str(param.default)
        rendered.append(
            {
                "name": param.name,
                "kind": _KIND_LABELS.get(param.kind, "positional_or_keyword"),
                "required": required,
                "annotation": info["label"],
                "input": info["input"],
                "default": default,
                "options": info["options"],
            },
        )
    return rendered


def _task_doc(task: object) -> str | None:
    return inspect.getdoc(task) or None


def _coerce_queue_value(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _queue_from_mapping(options: object) -> str | None:
    if isinstance(options, Mapping):
        return _coerce_queue_value(options.get("queue"))
    return None


def _app_default_queue(app: object | None) -> str | None:
    if app is None:
        return None
    conf = getattr(app, "conf", None)
    if conf is None:
        return None
    getter = getattr(conf, "get", None)
    if callable(getter):
        return _coerce_queue_value(getter("task_default_queue"))
    return _coerce_queue_value(getattr(conf, "task_default_queue", None))


def _task_default_queue(task: object | None, app: object | None) -> str | None:
    if task is None:
        return _app_default_queue(app)
    queue = _coerce_queue_value(getattr(task, "queue", None))
    if queue:
        return queue
    queue = _queue_from_mapping(getattr(task, "options", None))
    if queue:
        return queue
    exec_options = None
    getter = getattr(task, "_get_exec_options", None)
    if callable(getter):
        try:
            exec_options = getter()
        except Exception:  # noqa: BLE001
            exec_options = None
    queue = _queue_from_mapping(exec_options)
    if queue:
        return queue
    return _app_default_queue(app)


def _resolve_task_entry(apps: Sequence[Celery], task_name: str) -> tuple[object | None, Celery | None]:
    for app in apps:
        task_map = getattr(app, "tasks", None)
        if not isinstance(task_map, dict):
            continue
        task = task_map.get(task_name)
        if task is not None:
            return task, app
    return None, apps[0] if apps else None


def _build_task_schemas(apps: Sequence[Celery], task_names: Sequence[str]) -> dict[str, _TaskSchema]:
    lookup: dict[str, tuple[object, Celery]] = {}
    for app in apps:
        task_map = getattr(app, "tasks", None)
        if not isinstance(task_map, dict):
            continue
        for name, task in task_map.items():
            if name.startswith("celery."):
                continue
            lookup.setdefault(name, (task, app))

    schemas: dict[str, _TaskSchema] = {}
    for name in task_names:
        entry = lookup.get(name)
        if entry is None:
            schemas[name] = {"params": [], "doc": None, "queue": None}
            continue
        task, task_app = entry
        signature = _task_signature(task)
        queue = _task_default_queue(task, task_app)
        if signature is None:
            schemas[name] = {"params": [], "doc": _task_doc(task), "queue": queue}
            continue
        schemas[name] = {"params": _task_params(task, signature), "doc": _task_doc(task), "queue": queue}
    return schemas


def _build_tasks(filters: TaskFilter | None = None) -> list[_TaskView]:
    with open_db() as db:
        tasks = db.get_tasks(filters)
    return [_task_to_view(task) for task in tasks]


def build_tasks(filters: TaskFilter | None = None) -> list[_TaskView]:
    """Build task entries for view and API consumers."""
    return _build_tasks(filters)


def build_task_schemas(apps: Sequence[Celery], task_names: Sequence[str]) -> dict[str, _TaskSchema]:
    """Build task schema metadata for UI consumers."""
    return _build_task_schemas(apps, task_names)


def _filter_tasks(tasks: Sequence[_TaskView], search_term: str) -> list[_TaskView]:
    if not search_term:
        return list(tasks)
    needle = search_term.lower()
    filtered: list[_TaskView] = []
    for task in tasks:
        haystack = f"{task['name']} {task['task_id']} {task['worker']} {task['args']} {task['kwargs']}".lower()
        if needle in haystack:
            filtered.append(task)
    return filtered


def _normalize_sort(sort_key: str | None, sort_dir: str | None) -> tuple[str, str]:
    key = (sort_key or "").strip()
    direction = (sort_dir or "").strip().lower()
    if key not in _SORTABLE_FIELDS:
        return "", "desc"
    if direction not in _SORT_DIRECTIONS:
        direction = "desc"
    return key, direction


def _sort_value(task: _TaskView, field: str) -> str | float | int | datetime | None:
    value = task.get(field)
    if isinstance(value, str):
        return value.lower()
    if isinstance(value, (int, float, datetime)):
        return value
    return None


def _sort_tasks(tasks: Sequence[_TaskView], field: str, direction: str) -> list[_TaskView]:
    reverse = direction == "desc"

    def _compare(left: _TaskView, right: _TaskView) -> int:  # noqa: PLR0911
        left_value = _sort_value(left, field)
        right_value = _sort_value(right, field)
        if left_value is None and right_value is None:
            return 0
        if left_value is None:
            return 1
        if right_value is None:
            return -1
        if left_value == right_value:
            return 0
        if isinstance(left_value, str) and isinstance(right_value, str):
            if reverse:
                return -1 if left_value > right_value else 1
            return -1 if left_value < right_value else 1
        if isinstance(left_value, datetime) and isinstance(right_value, datetime):
            if reverse:
                return -1 if left_value > right_value else 1
            return -1 if left_value < right_value else 1
        if isinstance(left_value, (int, float)) and isinstance(right_value, (int, float)):
            if reverse:
                return -1 if left_value > right_value else 1
            return -1 if left_value < right_value else 1
        return 0

    return sorted(tasks, key=cmp_to_key(_compare))


def _build_sort_headers(
    request: HttpRequest,
    sort_key: str,
    sort_dir: str,
) -> dict[str, _SortHeader]:
    headers: dict[str, _SortHeader] = {}
    for key, label in _SORTABLE_FIELDS.items():
        query = request.GET.copy()
        query.pop("page", None)
        next_dir = "desc"
        if sort_key == key and sort_dir == "desc":
            next_dir = "asc"
        query["sort"] = key
        query["dir"] = next_dir
        url = f"{request.path}?{query.urlencode()}"
        headers[key] = {
            "key": key,
            "label": label,
            "url": url,
            "active": sort_key == key,
            "direction": sort_dir if sort_key == key else "",
        }
    return headers


def _build_stats_sort_headers(
    request: HttpRequest,
    sort_key: str,
    sort_dir: str,
) -> dict[str, _SortHeader]:
    headers: dict[str, _SortHeader] = {}
    for key, label in _STATS_SORT_FIELDS.items():
        query = request.GET.copy()
        query["tab"] = "stats"
        query["stats_sort"] = key
        next_dir = "desc"
        if sort_key == key and sort_dir == "desc":
            next_dir = "asc"
        query["stats_dir"] = next_dir
        url = f"{request.path}?{query.urlencode()}"
        headers[key] = {
            "key": key,
            "label": label,
            "url": url,
            "active": sort_key == key,
            "direction": sort_dir if sort_key == key else "",
        }
    return headers


def _normalize_stats_sort(sort_key: str | None, sort_dir: str | None) -> tuple[str, str]:
    key = (sort_key or "").strip()
    direction = (sort_dir or "").strip().lower()
    if key not in _STATS_SORT_FIELDS:
        key = "avg"
    if direction not in _SORT_DIRECTIONS:
        direction = "desc"
    return key, direction


def _sort_stats_rows(rows: list[dict[str, object]], key: str, direction: str) -> list[dict[str, object]]:
    reverse = direction == "desc"

    def _value(row: dict[str, object]) -> object:
        value = row.get(key)
        if isinstance(value, str):
            return value.lower()
        return value

    def _key_fn(row: dict[str, object]) -> tuple[int, object]:
        value = _value(row)
        is_none = 1 if value is None else 0
        return (is_none, value if not isinstance(value, (int, float)) else float(value))

    return sorted(rows, key=_key_fn, reverse=reverse)


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if pct <= 0:
        return values[0]
    if pct >= 1:
        return values[-1]
    index = (len(values) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(values) - 1)
    if lower == upper:
        return values[lower]
    weight = index - lower
    return values[lower] + (values[upper] - values[lower]) * weight


def _build_stats_rows(tasks: Sequence[Task]) -> list[dict[str, object]]:
    counts: dict[str, int] = {}
    runtimes: dict[str, list[float]] = {}
    for task in tasks:
        name = task.name or "unknown"
        counts[name] = counts.get(name, 0) + 1
        if task.runtime is not None:
            runtimes.setdefault(name, []).append(float(task.runtime))

    rows: list[dict[str, object]] = []
    for name, count in counts.items():
        values = sorted(runtimes.get(name, []))
        if values:
            min_runtime = values[0]
            max_runtime = values[-1]
            avg_runtime = sum(values) / len(values)
            p95 = _percentile(values, 0.95)
            p99 = _percentile(values, 0.99)
        else:
            min_runtime = None
            max_runtime = None
            avg_runtime = None
            p95 = None
            p99 = None
        rows.append(
            {
                "name": name,
                "count": count,
                "min": min_runtime,
                "max": max_runtime,
                "avg": avg_runtime,
                "p95": p95,
                "p99": p99,
            },
        )
    return rows


def task_list(request: HttpRequest) -> HttpResponse:  # noqa: PLR0912, PLR0915
    """Render the task list page."""
    now = timezone.now()
    default_tab = request.GET.get("tab", "queue")
    state_raw = request.GET.get("state", "")
    state_values = [value.strip().upper() for value in state_raw.split(",") if value.strip()]
    filter_state = state_values[0] if len(state_values) == 1 else ""
    filter_task_name = request.GET.get("task_name", "")
    filter_worker = request.GET.get("worker", "")
    search_term = request.GET.get("search", "").strip()
    start_raw = request.GET.get("start")
    end_raw = request.GET.get("end")
    start_filter = _parse_date(start_raw)
    end_filter = _parse_date(end_raw)
    if end_filter is not None and _is_date_only(end_raw):
        end_filter = end_filter + timedelta(days=1) - timedelta(microseconds=1)

    time_range = None
    if start_filter or end_filter:
        range_start = start_filter or now - timedelta(days=365)
        range_end = end_filter or now
        time_range = TimeRange(start=range_start, end=range_end)

    task_filter = TaskFilter(
        task_name=filter_task_name or None,
        state=filter_state or None,
        worker=filter_worker or None,
        time_range=time_range,
        search=search_term or None,
    )

    sort_key, sort_dir = _normalize_sort(request.GET.get("sort"), request.GET.get("dir"))
    sort_headers = _build_sort_headers(request, sort_key, sort_dir)

    page_size = _DEFAULT_PAGE_SIZE
    try:
        page_size = int(request.GET.get("page_size", str(_DEFAULT_PAGE_SIZE)))
    except ValueError:
        page_size = _DEFAULT_PAGE_SIZE
    if page_size not in _PAGE_SIZE_OPTIONS:
        page_size = _DEFAULT_PAGE_SIZE
    try:
        page = max(int(request.GET.get("page", "1")), 1)
    except ValueError:
        page = 1

    total_count = 0
    total_pages = 1
    paginated_tasks: list[_TaskView] = []
    use_paged_query = len(state_values) <= 1
    if use_paged_query:
        offset = (page - 1) * page_size
        with open_db() as db:
            task_rows, total_count = db.get_tasks_page(
                task_filter,
                sort_key=sort_key or None,
                sort_dir=sort_dir if sort_key else None,
                limit=page_size,
                offset=offset,
            )
        total_pages = max(math.ceil(total_count / page_size), 1)
        if total_count > 0 and page > total_pages:
            page = total_pages
            offset = (page - 1) * page_size
            with open_db() as db:
                task_rows, total_count = db.get_tasks_page(
                    task_filter,
                    sort_key=sort_key or None,
                    sort_dir=sort_dir if sort_key else None,
                    limit=page_size,
                    offset=offset,
                )
        paginated_tasks = [_task_to_view(task) for task in task_rows]
    else:
        tasks = _build_tasks(task_filter)
        if len(state_values) > 1:
            state_set = set(state_values)
            tasks = [task for task in tasks if task["state"] in state_set]
        if sort_key:
            tasks = _sort_tasks(tasks, sort_key, sort_dir)
        total_count = len(tasks)
        total_pages = max(math.ceil(total_count / page_size), 1)
        page = min(page, total_pages)
        start_index = (page - 1) * page_size
        end_index = start_index + page_size
        paginated_tasks = tasks[start_index:end_index]

    if total_count == 0:
        visible_start = 0
        visible_end = 0
    else:
        visible_start = (page - 1) * page_size + 1
        visible_end = visible_start + max(len(paginated_tasks) - 1, 0)

    _attach_child_counts(paginated_tasks)

    query = request.GET.copy()
    query.pop("page", None)
    base_query = query.urlencode()

    def _page_url(target: int) -> str:
        query_args = f"{base_query}&page={target}" if base_query else f"page={target}"
        return f"{request.path}?{query_args}"

    def _tab_url(query: QueryDict, tab_id: str) -> str:
        encoded = query.urlencode()
        base = f"{request.path}?{encoded}" if encoded else request.path
        return f"{base}#tab-{tab_id}"

    queue_query = request.GET.copy()
    queue_query["tab"] = "queue"
    for key in ("stats_task", "stats_sort", "stats_dir"):
        queue_query.pop(key, None)
    stats_query = request.GET.copy()
    stats_query["tab"] = "stats"
    for key in (
        "page",
        "page_size",
        "sort",
        "dir",
        "task_name",
        "state",
        "worker",
        "search",
        "start",
        "end",
    ):
        stats_query.pop(key, None)

    queue_tab_url = _tab_url(queue_query, "queue")
    stats_tab_url = _tab_url(stats_query, "stats")

    active_tab = default_tab if default_tab in {"queue", "stats"} else "queue"
    build_stats = active_tab == "stats"
    with open_db() as db:
        worker_rows = db.get_workers()
        task_name_options = db.list_task_names()
        all_tasks = db.get_tasks() if build_stats else []
    workers = sorted({worker.hostname for worker in worker_rows})

    stats_task = request.GET.get("stats_task", "").strip()
    stats_rows: list[dict[str, object]] = []
    stats_options: list[str] = []
    if build_stats:
        stats_rows = _build_stats_rows(all_tasks)
        if stats_task:
            stats_rows = [row for row in stats_rows if row["name"] == stats_task]
        for row in stats_rows:
            row["link"] = f"{request.path}?search={quote_plus(str(row['name']))}"
        stats_options = sorted({task.name or "unknown" for task in all_tasks})
    stats_sort_key, stats_sort_dir = _normalize_stats_sort(
        request.GET.get("stats_sort"),
        request.GET.get("stats_dir"),
    )
    stats_rows = _sort_stats_rows(stats_rows, stats_sort_key, stats_sort_dir)
    stats_sort_headers = _build_stats_sort_headers(request, stats_sort_key, stats_sort_dir)

    context = {
        "title": "Tasks",
        "tasks": paginated_tasks,
        "filters": {
            "task_name": filter_task_name,
            "state": filter_state,
            "worker": filter_worker,
            "search": search_term,
            "start": start_raw or "",
            "end": end_raw or "",
        },
        "utc_now": now,
        "sort": {
            "key": sort_key,
            "direction": sort_dir,
        },
        "sort_headers": sort_headers,
        "state_options": STATE_OPTIONS,
        "task_name_options": task_name_options,
        "worker_options": workers,
        "page_info": {
            "current": page,
            "total": total_pages,
            "size": page_size,
            "total_count": total_count,
            "visible_start": visible_start,
            "visible_end": visible_end,
        },
        "page_size_options": _PAGE_SIZE_OPTIONS,
        "prev_url": _page_url(page - 1) if page > 1 else None,
        "next_url": _page_url(page + 1) if page < total_pages else None,
        "queue_tab_url": queue_tab_url,
        "stats_tab_url": stats_tab_url,
        "stats": {
            "search": stats_task,
            "rows": stats_rows,
            "options": stats_options,
            "sort": {
                "key": stats_sort_key,
                "direction": stats_sort_dir,
            },
            "sort_headers": stats_sort_headers,
        },
        "default_tab": active_tab,
    }
    return render(request, "tasks/list.html", context)


def _fetch_task(task_id: str) -> Task | None:
    with open_db() as db:
        return db.get_task(task_id)


def fetch_task(task_id: str, _tasks: Sequence[_TaskEntry] | None = None) -> _TaskEntry | None:
    """Return a task entry by ID."""
    task = _fetch_task(task_id)
    if task is None:
        return None
    return _task_to_view(task)


def _build_relations(task: Task) -> list[dict[str, str]]:
    steps: list[dict[str, str]] = []
    if task.received is not None:
        steps.append({"title": "Received", "state": "RECEIVED"})
    if task.started is not None:
        steps.append({"title": "Started", "state": "STARTED"})
    if task.finished is not None:
        steps.append({"title": "Finished", "state": task.state})
    if not steps:
        steps.append({"title": "State", "state": task.state})
    return steps


def _build_task_link(db: BaseDBController, task_id: str) -> _TaskLink:
    task = db.get_task(task_id)
    if task is None:
        return {
            "id": task_id,
            "label": task_id,
            "state": None,
            "badge_class": "badge-muted",
            "exists": False,
        }
    return {
        "id": task.task_id,
        "label": task.name or task.task_id,
        "state": task.state,
        "badge_class": STATE_BADGES.get(task.state, "badge-muted"),
        "exists": True,
    }


def _parent_id(task: Task, relations: Sequence[TaskRelation]) -> str | None:
    if task.parent_id:
        return task.parent_id
    for relation in relations:
        if relation.child_id == task.task_id and relation.parent_id:
            return relation.parent_id
    return None


def _child_ids(task: Task, relations: Sequence[TaskRelation]) -> list[str]:
    children: set[str] = set()
    for relation in relations:
        if relation.parent_id != task.task_id:
            continue
        for child_id in _expand_task_ids(relation.child_id):
            if child_id and child_id != task.task_id:
                children.add(child_id)
    return sorted(children)


def _expand_task_ids(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if item is not None]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text.startswith(("[", "(")):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                try:
                    parsed = ast.literal_eval(text)
                except (SyntaxError, ValueError):
                    parsed = text
            if isinstance(parsed, (list, tuple, set)):
                return [str(item) for item in parsed if item is not None]
        return [text]
    return [str(value)]


def _attach_child_counts(tasks: list[_TaskView]) -> None:
    if not tasks:
        return
    root_groups: dict[str, set[str]] = {}
    child_sets: dict[str, set[str]] = {task["task_id"]: set() for task in tasks}
    for task in tasks:
        root_id = task["root_id"] or task["task_id"]
        root_groups.setdefault(root_id, set()).add(task["task_id"])
    with open_db() as db:
        for root_id, parent_ids in root_groups.items():
            relations = db.get_task_relations(root_id)
            for relation in relations:
                parent_id = relation.parent_id
                if parent_id is None or parent_id not in parent_ids:
                    continue
                for child_id in _expand_task_ids(relation.child_id):
                    if child_id and child_id != parent_id:
                        child_sets[parent_id].add(child_id)
    for task in tasks:
        task["child_count"] = len(child_sets.get(task["task_id"], set()))


def build_relations(task: _TaskEntry) -> list[dict[str, str]]:
    """Return relation steps for a task."""
    task_obj = Task(
        task_id=task["task_id"],
        name=task["name"],
        state=task["state"],
        worker=task["worker"],
        received=task.get("received"),
        started=task.get("started"),
        finished=task.get("finished"),
        runtime=task["runtime"],
        args=task["args"],
        kwargs=task["kwargs"],
        result=task["result"],
        traceback=task["traceback"],
        retries=task["retries"],
        parent_id=task["parent_id"],
        root_id=task["root_id"],
        group_id=task["group_id"],
        chord_id=task["chord_id"],
    )
    return _build_relations(task_obj)


def task_detail(request: HttpRequest, task_id: str) -> HttpResponse:
    """Render the task detail page."""
    task = _fetch_task(task_id)
    if task is None:
        return render(
            request,
            "404.html",
            {"title": "Task not found", "path": request.path},
            status=404,
        )
    task_view = _task_to_view(task)

    root_id = task.root_id or task.task_id
    with open_db() as db:
        relations = db.get_task_relations(root_id)
        parent = _parent_id(task, relations)
        parent_link = _build_task_link(db, parent) if parent else None
        child_links = [_build_task_link(db, child_id) for child_id in _child_ids(task, relations)]

    show_traceback = bool(task.traceback) or task.state in {"FAILURE", "RETRY"}
    context = {
        "title": "Task detail",
        "task": task_view,
        "state_badge": STATE_BADGES.get(task.state, "badge-muted"),
        "relations": _build_relations(task),
        "parent_link": parent_link,
        "child_links": child_links,
        "show_traceback": show_traceback,
    }
    return render(request, "tasks/detail.html", context)


def task_submit(request: HttpRequest) -> HttpResponse:
    """Render and handle the submit task form."""
    registry = get_registry()
    apps = registry.get_apps()
    task_names = list_task_names(apps)
    task_schemas = _build_task_schemas(apps, task_names)

    if request.method == "POST":
        task_name = request.POST.get("task_name", "").strip()
        if not task_name:
            return HttpResponseBadRequest(_TASK_NAME_REQUIRED)
        args_raw = request.POST.get("args", "")
        kwargs_raw = request.POST.get("kwargs", "")
        queue = request.POST.get("queue", "").strip() or None
        repeat_raw = request.POST.get("repeat", "1")
        try:
            repeat = max(1, min(int(repeat_raw), 100))
        except ValueError:
            repeat = 1

        args = _parse_args(args_raw)
        kwargs = _parse_kwargs(kwargs_raw)

        if not apps:
            return HttpResponseBadRequest("No Celery workers configured")
        task_obj, target_app = _resolve_task_entry(apps, task_name)
        if target_app is None:
            return HttpResponseBadRequest("No Celery workers configured")
        if queue is None:
            queue = _task_default_queue(task_obj, target_app)
        worker_name = app_name(target_app)
        for _ in range(repeat):
            task_control.send_task(
                registry,
                worker_name,
                task_name,
                args=args,
                kwargs=kwargs,
                queue=queue,
            )
        return redirect("tasks")

    return render(
        request,
        "tasks/submit.html",
        {
            "title": "Submit task",
            "task_names": task_names,
            "task_schemas": task_schemas,
        },
    )


@require_post
def task_retry(_request: HttpRequest, task_id: str) -> HttpResponse:
    """Re-dispatch a task with the same signature."""
    task = _fetch_task(task_id)
    if task is None:
        raise Http404(_TASK_NOT_FOUND)
    if not task.name:
        return HttpResponseBadRequest("Task name unavailable")

    registry = get_registry()
    apps = registry.get_apps()
    if not apps:
        return HttpResponseBadRequest("No Celery workers configured")

    args = _parse_args(task.args or "")
    kwargs = _parse_kwargs(task.kwargs or "")
    args = cast("list[object]", _strip_ellipsis(args))
    kwargs = cast("dict[str, object]", _strip_ellipsis(kwargs))
    worker_name = app_name(apps[0])
    task_control.send_task(
        registry,
        worker_name,
        task.name,
        args=args,
        kwargs=kwargs,
    )
    return redirect("task-detail", task_id=task_id)


@require_post
def task_revoke(_request: HttpRequest, task_id: str) -> HttpResponse:
    """Revoke a task by ID."""
    registry = get_registry()
    apps = registry.get_apps()
    if not apps:
        return HttpResponseBadRequest("No Celery workers configured")
    worker_name = app_name(apps[0])
    task_control.revoke(registry, worker_name, task_id)
    return redirect("task-detail", task_id=task_id)
