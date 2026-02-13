# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from django.test import Client


def test_dashboard_renders(web_client: Client) -> None:
    response = web_client.get("/")
    assert response.status_code == 200
    assert b"Dashboard" in response.content


def test_tasks_list_renders(web_client: Client) -> None:
    response = web_client.get("/tasks/")
    assert response.status_code == 200
    assert b"Task queue" in response.content


def test_task_detail_renders(web_client: Client) -> None:
    response = web_client.get("/tasks/task-0001/")
    assert response.status_code == 200
    assert b"Task lookup" in response.content


def test_task_graph_renders(web_client: Client) -> None:
    response = web_client.get("/tasks/task-0001/graph/")
    assert response.status_code == 200
    assert b"Task graph" in response.content


def test_workers_list_renders(web_client: Client) -> None:
    response = web_client.get("/workers/")
    assert response.status_code == 200
    assert b"Brokers & workers" in response.content


def test_worker_detail_renders(web_client: Client) -> None:
    response = web_client.get("/workers/alpha/")
    assert response.status_code == 200
    assert b"Worker detail" in response.content


def test_broker_page_renders(web_client: Client) -> None:
    response = web_client.get("/broker/")
    assert response.status_code == 302
    assert response.headers["Location"] == "/workers/"


def test_beat_page_renders(web_client: Client) -> None:
    response = web_client.get("/beat/")
    assert response.status_code == 200
    assert b"Periodic tasks" in response.content


def test_api_tasks_list(web_client: Client) -> None:
    response = web_client.get("/api/tasks/")
    assert response.status_code == 200
    assert response.json()["tasks"]


def test_api_task_detail(web_client: Client) -> None:
    response = web_client.get("/api/tasks/task-0001/")
    assert response.status_code == 200
    assert response.json()["task_id"] == "task-0001"


def test_api_task_relations(web_client: Client) -> None:
    response = web_client.get("/api/tasks/task-0001/relations/")
    assert response.status_code == 200
    assert "relations" in response.json()


def test_api_task_graph_snapshot(web_client: Client) -> None:
    response = web_client.get("/api/tasks/task-0001/graph/")
    assert response.status_code == 200
    payload = response.json()
    assert "meta" in payload
    assert "nodes" in payload
    assert "edges" in payload


def test_api_task_graph_updates(web_client: Client) -> None:
    response = web_client.get("/api/tasks/task-0001/graph/updates/")
    assert response.status_code == 200
    payload = response.json()
    assert "generated_at" in payload
    assert "node_updates" in payload
    assert "meta_counts" in payload


def test_api_workers(web_client: Client) -> None:
    response = web_client.get("/api/workers/")
    assert response.status_code == 200
    assert response.json()["workers"]
