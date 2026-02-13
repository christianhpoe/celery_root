# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Broker inspection views and helpers."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING
from urllib.parse import urlparse

from django.http import Http404, HttpResponseBadRequest
from django.shortcuts import redirect, render
from django.utils import timezone

from celery_root.components.web.services import app_name, get_registry, open_db
from celery_root.core.engine.brokers import list_queues, purge_queues

from .decorators import require_post

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from celery import Celery
    from django.http import HttpRequest, HttpResponse

    from celery_root.core.db.models import Worker
    from celery_root.core.registry import WorkerRegistry


_DEFAULT_BROKER_KEY = "default"
_BROKER_TYPE_LABELS: dict[str, str] = {
    "amqp": "RabbitMQ",
    "amqps": "RabbitMQ",
    "pyamqp": "RabbitMQ",
    "librabbitmq": "RabbitMQ",
    "redis": "Redis",
    "rediss": "Redis",
    "sqs": "Amazon SQS",
    "kafka": "Kafka",
    "memory": "In-memory",
    "sqla": "SQLAlchemy",
    "default": "Default",
    "custom": "Custom",
}
_UNKNOWN_BROKER_MESSAGE = "Unknown broker"


@dataclass(slots=True)
class QueueRow:
    """Row describing queue state on a broker."""

    name: str
    pending: int | None
    unacked: int | None
    total: int | None
    consumers: int | None
    rate: str | None


@dataclass(slots=True)
class SummaryItem:
    """Summary metric for a broker."""

    label: str
    value: str


@dataclass(slots=True)
class BrokerGroupRow:
    """Broker grouping metadata for overview/detail pages."""

    broker_url: str
    broker_label: str
    broker_key: str
    broker_type: str
    broker_type_label: str
    backend_labels: list[str]
    app_names: list[str]
    primary_app: str
    queues: list[QueueRow]
    summary: Sequence[SummaryItem]
    workers: list[str]


def _encode_broker_key(broker_url: str) -> str:
    if not broker_url:
        return _DEFAULT_BROKER_KEY
    encoded = base64.urlsafe_b64encode(broker_url.encode("utf-8")).decode("ascii")
    return encoded.rstrip("=")


def _decode_broker_key(key: str) -> str | None:
    if key == _DEFAULT_BROKER_KEY:
        return ""
    padded = key + ("=" * (-len(key) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return None


def _broker_type(broker_url: str) -> str:
    if not broker_url:
        return "default"
    scheme = urlparse(broker_url).scheme.lower()
    base = scheme.split("+", 1)[0] if scheme else ""
    return base or "custom"


def _broker_type_label(broker_url: str) -> str:
    broker_type = _broker_type(broker_url)
    return _BROKER_TYPE_LABELS.get(broker_type, broker_type.upper())


def encode_broker_key(broker_url: str) -> str:
    """Return a stable URL-safe key for a broker URL."""
    return _encode_broker_key(broker_url)


def broker_type_label(broker_url: str) -> str:
    """Return a display label for a broker transport."""
    return _broker_type_label(broker_url)


def _queue_rows_for_apps(
    registry: WorkerRegistry,
    apps: Sequence[Celery],
    *,
    include_counts: bool = True,
) -> list[QueueRow]:
    queues_by_name: dict[str, QueueRow] = {}
    for app in apps:
        worker_name = app_name(app)
        try:
            queues = list_queues(registry, worker_name, include_counts=include_counts)
        except Exception:  # noqa: BLE001  # pragma: no cover - broker failures handled gracefully
            queues = []
        for queue in queues:
            pending = queue.messages
            consumers = queue.consumers
            existing = queues_by_name.get(queue.name)
            if existing is None:
                total = pending if pending is not None else None
                queues_by_name[queue.name] = QueueRow(
                    name=queue.name,
                    pending=pending,
                    unacked=None,
                    total=total,
                    consumers=consumers,
                    rate=None,
                )
            else:
                if pending is not None:
                    if existing.pending is None:
                        existing.pending = pending
                    else:
                        existing.pending = max(pending, existing.pending)
                if consumers is not None and (existing.consumers is None or consumers > existing.consumers):
                    existing.consumers = consumers
                if existing.pending is not None and existing.unacked is not None:
                    existing.total = existing.pending + existing.unacked
                elif existing.pending is not None and existing.total is None:
                    existing.total = existing.pending
    return sorted(queues_by_name.values(), key=lambda row: row.name)


def queue_rows_for_apps(
    registry: WorkerRegistry,
    apps: Sequence[Celery],
    *,
    include_counts: bool = True,
) -> list[QueueRow]:
    """Return queue rows for a set of Celery apps."""
    return _queue_rows_for_apps(registry, apps, include_counts=include_counts)


def queue_rows_for_app(
    registry: WorkerRegistry,
    app: Celery,
    *,
    include_counts: bool = True,
) -> list[QueueRow]:
    """Return queue rows for a single Celery app."""
    return _queue_rows_for_apps(registry, [app], include_counts=include_counts)


def _broker_summary(rows: Sequence[QueueRow]) -> Sequence[SummaryItem]:
    total_queues = len(rows)
    pending = sum(row.pending or 0 for row in rows)
    consumers = sum(row.consumers or 0 for row in rows)
    idle_queues = sum(1 for row in rows if (row.consumers or 0) == 0)
    return [
        SummaryItem(label="Queues", value=str(total_queues)),
        SummaryItem(label="Pending tasks", value=str(pending)),
        SummaryItem(label="Consumers online", value=str(consumers)),
        SummaryItem(label="Queues without consumers", value=str(idle_queues)),
    ]


def _backend_labels(apps: Sequence[Celery]) -> list[str]:
    labels: list[str] = []
    for app in apps:
        backend = app.conf.result_backend
        label = str(backend) if backend else "disabled"
        if label not in labels:
            labels.append(label)
    return labels or ["disabled"]


def _worker_queue_map(workers: Sequence[Worker]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    all_workers: set[str] = set()
    for worker in workers:
        all_workers.add(worker.hostname)
        queues = worker.queues or []
        for queue in queues:
            mapping.setdefault(queue, set()).add(worker.hostname)
    mapping["__all__"] = all_workers
    return mapping


def _attached_workers(
    queue_rows: Sequence[QueueRow],
    worker_map: Mapping[str, set[str]],
    *,
    fallback_to_all: bool = True,
) -> set[str]:
    attached: set[str] = set()
    for queue in queue_rows:
        attached.update(worker_map.get(queue.name, set()))
    if not attached and fallback_to_all:
        attached.update(worker_map.get("__all__", set()))
    return attached


def _worker_broker_map(workers: Sequence[Worker]) -> tuple[dict[str, set[str]], list[Worker]]:
    mapping: dict[str, set[str]] = {}
    unknown: list[Worker] = []
    for worker in workers:
        if worker.broker_url:
            mapping.setdefault(worker.broker_url, set()).add(worker.hostname)
        else:
            unknown.append(worker)
    return mapping, unknown


def _resolve_broker_workers(
    broker_url: str,
    queue_rows: Sequence[QueueRow],
    workers: Sequence[Worker],
) -> list[str]:
    broker_map, unknown_workers = _worker_broker_map(workers)
    attached: set[str] = set(broker_map.get(broker_url, set()))
    if unknown_workers:
        queue_map = _worker_queue_map(unknown_workers)
        attached.update(_attached_workers(queue_rows, queue_map, fallback_to_all=False))
    return sorted(attached)


def list_broker_groups(*, include_counts: bool = True) -> list[BrokerGroupRow]:
    """Return broker groupings for the overview tree."""
    registry = get_registry()
    broker_groups = registry.get_brokers()
    if not broker_groups:
        return []
    with open_db() as db:
        workers = db.get_workers()
    groups: list[BrokerGroupRow] = []
    for broker_url, group in sorted(broker_groups.items(), key=lambda item: item[0]):
        apps = list(group.apps)
        if not apps:
            continue
        app_names = [app_name(app) for app in apps]
        queues = _queue_rows_for_apps(registry, apps, include_counts=include_counts)
        attached_workers = _resolve_broker_workers(broker_url, queues, workers)
        groups.append(
            BrokerGroupRow(
                broker_url=broker_url,
                broker_label=broker_url or "default",
                broker_key=_encode_broker_key(broker_url),
                broker_type=_broker_type(broker_url),
                broker_type_label=_broker_type_label(broker_url),
                backend_labels=_backend_labels(apps),
                app_names=app_names,
                primary_app=app_names[0],
                queues=queues,
                summary=_broker_summary(queues),
                workers=attached_workers,
            ),
        )
    return groups


def _resolve_app_name(registry: WorkerRegistry, requested: str | None) -> str | None:
    if requested:
        try:
            registry.get_app(requested)
        except KeyError:
            return None
        return requested
    apps = registry.get_apps()
    if not apps:
        return None
    return app_name(apps[0])


def broker(_request: HttpRequest) -> HttpResponse:
    """Redirect legacy broker overview to the workers overview."""
    return redirect("workers")


def broker_detail(request: HttpRequest, broker_key: str) -> HttpResponse:
    """Render a broker detail page based on broker transport."""
    registry = get_registry()
    broker_url = _decode_broker_key(broker_key)
    if broker_url is None:
        raise Http404(_UNKNOWN_BROKER_MESSAGE)
    broker_groups = registry.get_brokers()
    group = broker_groups.get(broker_url)
    if group is None:
        raise Http404(_UNKNOWN_BROKER_MESSAGE)
    apps = list(group.apps)
    app_names = [app_name(app) for app in apps]
    queues = _queue_rows_for_apps(registry, apps)
    with open_db() as db:
        workers = db.get_workers()
    broker_row = BrokerGroupRow(
        broker_url=broker_url,
        broker_label=broker_url or "default",
        broker_key=_encode_broker_key(broker_url),
        broker_type=_broker_type(broker_url),
        broker_type_label=_broker_type_label(broker_url),
        backend_labels=_backend_labels(apps),
        app_names=app_names,
        primary_app=app_names[0] if app_names else "",
        queues=queues,
        summary=_broker_summary(queues),
        workers=_resolve_broker_workers(broker_url, queues, workers),
    )
    template_map = {
        "amqp": "brokers/detail_rabbit.html",
        "amqps": "brokers/detail_rabbit.html",
        "pyamqp": "brokers/detail_rabbit.html",
        "librabbitmq": "brokers/detail_rabbit.html",
        "redis": "brokers/detail_redis.html",
        "rediss": "brokers/detail_redis.html",
        "kafka": "brokers/detail_kafka.html",
        "sqs": "brokers/detail_sqs.html",
    }
    template_name = template_map.get(broker_row.broker_type, "brokers/detail_generic.html")
    context = {
        "title": f"{broker_row.broker_type_label} broker",
        "broker": broker_row,
        "last_updated": timezone.now(),
    }
    return render(request, template_name, context)


@require_post
def broker_purge(request: HttpRequest) -> HttpResponse:
    """Purge a specific broker queue."""
    queue_name = request.POST.get("queue", "").strip()
    if not queue_name:
        return HttpResponseBadRequest("Queue name required")
    registry = get_registry()
    worker_name = _resolve_app_name(registry, request.POST.get("app"))
    if worker_name is None:
        return HttpResponseBadRequest("Unknown Celery app")
    purge_queues(registry, worker_name, queue=queue_name)
    return redirect("broker")


@require_post
def broker_purge_idle(_request: HttpRequest) -> HttpResponse:
    """Purge queues with no consumers."""
    registry = get_registry()
    worker_name = _resolve_app_name(registry, _request.POST.get("app"))
    if worker_name is None:
        return HttpResponseBadRequest("Unknown Celery app")
    queue_infos = list_queues(registry, worker_name)
    for info in queue_infos:
        if (info.consumers or 0) == 0:
            purge_queues(registry, worker_name, queue=info.name)
    return redirect("broker")
