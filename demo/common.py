# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Shared configuration for demo workloads."""

from __future__ import annotations

from typing import Any, cast

from celery import Celery, current_app, signals

_SIGNAL_INSTALLED: dict[str, bool] = {"value": False}


def create_celery_app(
    name: str,
    broker_url: str,
    backend_url: str | None,
    queue: str | None = None,
) -> Celery:
    """Configure a Celery app for demos.

    Defaults are tuned to exercise the event stream and inspect
    infrastructure easily.
    """
    app = Celery(name, broker=broker_url, backend=backend_url)
    settings: dict[str, object] = {
        "worker_enable_remote_control": True,
        "broker_connection_retry_on_startup": True,
        "worker_max_tasks_per_child": 10,
        "worker_soft_shutdown_timeout": 30,
        "enable_utc": True,
        "worker_hijack_root_logger": True,
        "task_reject_on_worker_lost": True,
        "task_remote_tracebacks": True,
        "broker_connection_max_retries": None,
        "task_create_missing_queues": True,
        "task_acks_late": True,
        "result_backend_thread_safe": True,
        "result_backend_always_retry": True,
        "worker_send_task_events": True,
        "task_send_sent_event": True,
        "worker_cancel_long_running_tasks_on_connection_loss": True,
        "database_create_tables_at_setup": True,
        "database_short_lived_sessions": True,
    }
    if queue is not None:
        settings["task_default_queue"] = queue
    app.conf.update(settings)
    _install_task_sent_signal()
    return app


def _install_task_sent_signal() -> None:
    if _SIGNAL_INSTALLED["value"]:
        return
    _SIGNAL_INSTALLED["value"] = True


@signals.before_task_publish.connect(weak=False, dispatch_uid="celery_root_demo_task_sent")
def _publish_task_sent(
    _sender: str | None = None,
    headers: dict[str, object] | None = None,
    **_: object,
) -> None:
    if headers is None:
        return
    stamped_headers = headers.get("stamped_headers")
    stamps: dict[str, object] | None = None
    if isinstance(stamped_headers, list | tuple):
        stamps = {key: headers[key] for key in stamped_headers if isinstance(key, str) and key in headers} or None
    app = current_app
    dispatcher_factory = cast("Any", app.events).default_dispatcher
    with dispatcher_factory() as dispatcher:
        dispatcher.send(
            "task-sent",
            uuid=headers.get("id"),
            task=headers.get("task"),
            name=headers.get("task"),
            root_id=headers.get("root_id"),
            parent_id=headers.get("parent_id"),
            group_id=headers.get("group"),
            chord_id=headers.get("chord"),
            stamps=stamps,
            retries=headers.get("retries", 0),
            eta=headers.get("eta"),
            expires=headers.get("expires"),
            args=headers.get("argsrepr"),
            kwargs=headers.get("kwargsrepr"),
        )


def publish_task_relation(
    *,
    app: Celery,
    root_id: str,
    parent_id: str | None,
    child_id: str,
    relation: str,
) -> None:
    """Publish a relation event for the Root event listener."""
    dispatcher_factory = cast("Any", app.events).default_dispatcher
    with dispatcher_factory() as dispatcher:
        dispatcher.send(
            "task-relation",
            root_id=root_id,
            parent_id=parent_id,
            child_id=child_id,
            relation=relation,
        )


def publish_canvas_task(  # noqa: PLR0913
    *,
    app: Celery,
    task_id: str,
    name: str,
    state: str = "sent",
    root_id: str | None = None,
    parent_id: str | None = None,
    stamps: dict[str, object] | None = None,
) -> None:
    """Publish a lightweight task event for virtual canvas nodes."""
    event_type = f"task-{state}"
    dispatcher_factory = cast("Any", app.events).default_dispatcher
    with dispatcher_factory() as dispatcher:
        dispatcher.send(
            event_type,
            uuid=task_id,
            task=name,
            name=name,
            root_id=root_id or task_id,
            parent_id=parent_id,
            stamps=stamps,
        )
