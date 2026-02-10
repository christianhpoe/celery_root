from __future__ import annotations

from datetime import UTC, datetime

from prometheus_client import CollectorRegistry

from celery_cnc.components.metrics.opentelemetry import OTelExporter
from celery_cnc.components.metrics.prometheus import PrometheusExporter
from celery_cnc.core.db.models import TaskEvent, TaskStats, WorkerEvent


def test_prometheus_exporter_records_metrics() -> None:
    registry = CollectorRegistry()
    exporter = PrometheusExporter(registry=registry)

    exporter.on_task_event(
        TaskEvent(
            task_id="t1",
            name="demo.add",
            state="SUCCESS",
            timestamp=datetime.now(UTC),
            worker="w1",
            runtime=0.25,
        ),
    )
    exporter.on_worker_event(WorkerEvent(hostname="w1", event="online", timestamp=datetime.now(UTC), info=None))
    exporter.update_stats(TaskStats(min_runtime=0.1, max_runtime=1.0, avg_runtime=0.4, p95=0.8, p99=0.95))

    total = registry.get_sample_value(
        "celery_cnc_events_total",
        labels={
            "task": "demo.add",
            "type": "task-succeeded",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert total == 1

    runtime_count = registry.get_sample_value(
        "celery_cnc_task_runtime_seconds_count",
        labels={
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert runtime_count == 1

    online = registry.get_sample_value(
        "celery_cnc_worker_online",
        labels={
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert online == 1


def test_prometheus_exporter_flower_compatibility_prefix() -> None:
    registry = CollectorRegistry()
    exporter = PrometheusExporter(registry=registry, flower_compatibility=True)

    exporter.on_task_event(
        TaskEvent(
            task_id="t1",
            name="demo.add",
            state="SUCCESS",
            timestamp=datetime.now(UTC),
            worker="w1",
            runtime=0.25,
        ),
    )

    total = registry.get_sample_value(
        "flower_events_total",
        labels={
            "task": "demo.add",
            "type": "task-succeeded",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert total == 1


def test_otel_exporter_records_spans() -> None:
    exporter = OTelExporter(service_name="test-service")

    exporter.on_task_event(
        TaskEvent(
            task_id="t2",
            name="demo.mul",
            state="FAILURE",
            timestamp=datetime.now(UTC),
            worker="w2",
            runtime=1.2,
        ),
    )
    exporter.on_worker_event(WorkerEvent(hostname="w2", event="offline", timestamp=datetime.now(UTC), info=None))
    exporter.update_stats(TaskStats(count=2, min_runtime=0.2, max_runtime=1.2, avg_runtime=0.7))

    spans = exporter.exporter.get_finished_spans()
    assert len(spans) == 3
    names = {span.name for span in spans}
    assert {"task.event", "worker.event", "task.stats"}.issubset(names)
