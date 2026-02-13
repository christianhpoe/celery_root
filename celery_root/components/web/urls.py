# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Django URL patterns for the Celery Root web app."""

from __future__ import annotations

from django.urls import path

from . import auth
from .views import api, beat, broker, dashboard, errors, graphs, logs, metrics, settings, system, tasks, workers

urlpatterns = [
    path("login", auth.login, name="login"),
    path("logout", auth.logout, name="logout"),
    path("healthcheck", system.healthcheck, name="healthcheck"),
    path("metrics", system.metrics, name="metrics"),
    path("prometheus/", metrics.prometheus, name="prometheus"),
    path("opentelemetry/", metrics.opentelemetry, name="opentelemetry"),
    path("", dashboard.dashboard, name="dashboard"),
    path("dashboard/fragment/", dashboard.dashboard_fragment, name="dashboard-fragment"),
    path("tasks/", tasks.task_list, name="tasks"),
    path("tasks/submit/", tasks.task_submit, name="task-submit"),
    path("tasks/<str:task_id>/", tasks.task_detail, name="task-detail"),
    path("tasks/<str:task_id>/retry/", tasks.task_retry, name="task-retry"),
    path("tasks/<str:task_id>/revoke/", tasks.task_revoke, name="task-revoke"),
    path("tasks/<str:task_id>/graph/", graphs.task_graph, name="task-graph"),
    path("api/tasks/<str:task_id>/graph/", graphs.task_graph_api, name="task-graph-api"),
    path(
        "api/tasks/<str:task_id>/graph/updates/",
        graphs.task_graph_updates_api,
        name="task-graph-updates",
    ),
    path("workers/", workers.worker_list, name="workers"),
    path("workers/fragment/", workers.worker_list_fragment, name="workers-fragment"),
    path("workers/add/", workers.worker_add, name="worker-add"),
    path("workers/restart/", workers.workers_restart, name="workers-restart"),
    path("workers/<str:hostname>/", workers.worker_detail, name="worker-detail"),
    path(
        "workers/<str:hostname>/fragment/",
        workers.worker_detail_fragment,
        name="worker-detail-fragment",
    ),
    path("workers/<str:hostname>/grow/", workers.worker_grow, name="worker-grow"),
    path("workers/<str:hostname>/shrink/", workers.worker_shrink, name="worker-shrink"),
    path("workers/<str:hostname>/autoscale/", workers.worker_autoscale, name="worker-autoscale"),
    path("broker/", broker.broker, name="broker"),
    path("brokers/<str:broker_key>/", broker.broker_detail, name="broker-detail"),
    path("broker/purge/", broker.broker_purge, name="broker-purge"),
    path("broker/purge-idle/", broker.broker_purge_idle, name="broker-purge-idle"),
    path("beat/", beat.beat, name="beat"),
    path("beat/schedule/add/", beat.beat_add, name="beat-add"),
    path("beat/schedule/<str:schedule_id>/edit/", beat.beat_edit, name="beat-edit"),
    path("beat/schedule/<str:schedule_id>/delete/", beat.beat_delete, name="beat-delete"),
    path("beat/schedule/sync/", beat.beat_sync, name="beat-sync"),
    path("logs/", logs.logs, name="logs"),
    path("settings/", settings.settings_page, name="settings"),
    path("api/workers/", api.worker_list, name="api-workers"),
    path("api/tasks/", api.tasks, name="api-tasks"),
    path("api/tasks/<str:task_id>/", api.task_detail, name="api-task-detail"),
    path("api/tasks/<str:task_id>/relations/", api.task_relations, name="api-task-relations"),
    path("api/events/latest/", api.events_latest, name="api-events"),
    path("api/beat/schedules/", api.beat_schedules, name="api-beat-schedules"),
    path("api/components/", system.components, name="api-components"),
]

handler404 = errors.handler404
