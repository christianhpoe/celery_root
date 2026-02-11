from __future__ import annotations

from datetime import UTC, datetime, timedelta

from opentelemetry.sdk.metrics.export import (
    Gauge,
    Histogram,
    HistogramDataPoint,
    InMemoryMetricReader,
    Metric,
    MetricsData,
    Sum,
)
from prometheus_client import CollectorRegistry

from celery_cnc.components.metrics.opentelemetry import OTelExporter
from celery_cnc.components.metrics.prometheus import PrometheusExporter
from celery_cnc.core.db.models import TaskEvent, TaskStats, WorkerEvent


def _find_metric(data: MetricsData, name: str) -> Metric:
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name == name:
                    return metric
    msg = f"Metric {name} not found"
    raise AssertionError(msg)


def _get_number(metric: Metric, attributes: dict[str, str]) -> float:
    data = metric.data
    if isinstance(data, (Sum, Gauge)):
        for point in data.data_points:
            if point.attributes == attributes:
                return float(point.value)
    msg = f"No datapoint found for {metric.name} with {attributes}"
    raise AssertionError(msg)


def _get_histogram(metric: Metric, attributes: dict[str, str]) -> HistogramDataPoint:
    data = metric.data
    if isinstance(data, Histogram):
        for point in data.data_points:
            if point.attributes == attributes:
                return point
    msg = f"No histogram datapoint found for {metric.name} with {attributes}"
    raise AssertionError(msg)


def _require_metrics_data(reader: InMemoryMetricReader) -> MetricsData:
    data = reader.get_metrics_data()
    assert data is not None
    return data


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
    runtime_by_task_count = registry.get_sample_value(
        "celery_cnc_task_runtime_by_task_seconds_count",
        labels={
            "task": "demo.add",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert runtime_by_task_count == 1

    online = registry.get_sample_value(
        "celery_cnc_worker_online",
        labels={
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert online == 1


def test_prometheus_exporter_records_queue_latency_and_failures() -> None:
    registry = CollectorRegistry()
    exporter = PrometheusExporter(registry=registry)
    base = datetime.now(UTC)

    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="RECEIVED",
            timestamp=base,
            worker="w1",
        ),
    )
    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="STARTED",
            timestamp=base + timedelta(seconds=2),
            worker="w1",
        ),
    )
    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="FAILURE",
            timestamp=base + timedelta(seconds=3),
            worker="w1",
        ),
    )
    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="RETRY",
            timestamp=base + timedelta(seconds=4),
            worker="w1",
        ),
    )

    latency_count = registry.get_sample_value(
        "celery_cnc_task_queue_latency_seconds_count",
        labels={
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert latency_count == 1

    failures = registry.get_sample_value(
        "celery_cnc_task_failures_total",
        labels={
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert failures == 1

    retries = registry.get_sample_value(
        "celery_cnc_task_retries_total",
        labels={
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert retries == 1


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


def test_otel_exporter_records_metrics() -> None:
    reader = InMemoryMetricReader()
    exporter = OTelExporter(service_name="test-service", metric_reader=reader)

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
    exporter.force_flush()

    data = _require_metrics_data(reader)
    total = _get_number(
        _find_metric(data, "celery_cnc_events_total"),
        {
            "task": "demo.mul",
            "type": "task-failed",
            "worker": "w2",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert total == 1

    runtime = _get_histogram(
        _find_metric(data, "celery_cnc_task_runtime_seconds"),
        {
            "task": "demo.mul",
            "worker": "w2",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert runtime.count == 1

    online = _get_number(
        _find_metric(data, "celery_cnc_worker_online"),
        {
            "worker": "w2",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert online == 0


def test_otel_exporter_records_queue_latency_and_failures() -> None:
    reader = InMemoryMetricReader()
    exporter = OTelExporter(service_name="test-service", metric_reader=reader)
    base = datetime.now(UTC)

    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="RECEIVED",
            timestamp=base,
            worker="w1",
        ),
    )
    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="STARTED",
            timestamp=base + timedelta(seconds=2),
            worker="w1",
        ),
    )
    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="FAILURE",
            timestamp=base + timedelta(seconds=3),
            worker="w1",
        ),
    )
    exporter.on_task_event(
        TaskEvent(
            task_id="t3",
            name="demo.add",
            state="RETRY",
            timestamp=base + timedelta(seconds=4),
            worker="w1",
        ),
    )
    exporter.force_flush()

    data = _require_metrics_data(reader)
    latency = _get_histogram(
        _find_metric(data, "celery_cnc_task_queue_latency_seconds"),
        {
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert latency.count == 1

    failures = _get_number(
        _find_metric(data, "celery_cnc_task_failures_total"),
        {
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert failures == 1

    retries = _get_number(
        _find_metric(data, "celery_cnc_task_retries_total"),
        {
            "task": "demo.add",
            "worker": "w1",
            "broker": "unknown",
            "backend": "unknown",
        },
    )
    assert retries == 1
