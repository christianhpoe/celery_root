<!--
SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
SPDX-FileCopyrightText: 2026 Maximilian Dolling
SPDX-FileContributor: AUTHORS.md

SPDX-License-Identifier: BSD-3-Clause
-->

[![PyPI](https://img.shields.io/pypi/v/celery_root.svg)](https://pypi.org/project/celery_root/)
[![Python](https://img.shields.io/pypi/pyversions/celery_root.svg)](https://pypi.org/project/celery_root/)
[![Docs](https://img.shields.io/badge/docs-celeryroot.eu-blue)](https://docs.celeryroot.eu/)


# Celery Root

Docs: https://docs.celeryroot.eu

Celery Root is a control plane for Celery. It provides a Django-based UI, an event listener/collector, and helper utilities for inspecting tasks, workers, queues, and beat schedules. The Python package and distribution remain `celery_root` for compatibility.

## Features

- Task list with filtering, sorting, and detail views (args/kwargs/result/traceback).
- Task relation graph visualization (chains, groups, chords, maps).
- Worker fleet overview and per-worker drill-down.
- Broker queue inspection and purge actions.
- Beat schedule overview and editor.
- Pluggable storage (SQLite by default).

## Quickstart (demo)

Requirements: Python >= 3.13, `uv`, and Docker (for the demo broker/redis).

```bash
export CELERY_ROOT_WORKERS="your_app.celery:app,another_app.celery:app"
```

Start the supervisor + UI (standalone):

```bash
celery_root -A your_app.celery:app
```

Or run as a Celery subcommand:

```bash
celery -A your_app.celery:app celery_root
```

By default the UI binds to `127.0.0.1:8000`.

## Demo stack

Requirements: Python >= 3.10, `uv`, and Docker (for the demo broker/redis).

```bash
make demo-infra
make demo-worker-math
make demo-worker-text
make demo-root
```

Then open `http://127.0.0.1:8000`.

To enqueue demo tasks:

```bash
make demo-tasks
```

## Installation (repo)

Celery Root is currently built and run from this repository.

```bash
make install
```

This runs:

- `uv sync --all-extras --dev --frozen`
- `uv run pre-commit install`
- `npm --prefix frontend/graph-ui install`

Build the frontend assets:

```bash
celery_root -A demo.worker_math:app
```

Via Celery:

```bash
celery -A demo.worker_math:app celery_root
```

## Optional dependencies

Celery Root ships optional components behind extras. Install only what you need.

- `web`: Django-based UI.
- `mcp`: MCP server (FastMCP + Uvicorn) and Django for ASGI integration.
- `prometheus`: Prometheus metrics exporter.
- `otel`: OpenTelemetry exporter.

Install with `uv`:

```bash
uv sync --extra web --extra prometheus
```

Or install all extras:

```bash
uv sync --all-extras
```

Editable install with pip:

```bash
pip install -e ".[web,prometheus]"
```

## Configuration

Configuration is explicit via Pydantic models. Components are enabled when their config is provided (set to `None` to disable).

```python
from pathlib import Path

from celery_root import (
    BeatConfig,
    CeleryRootConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    OpenTelemetryConfig,
    PrometheusConfig,
)

config = CeleryRootConfig(
    database=DatabaseConfigSqlite(db_path=Path("./celery_root.db")),
    beat=BeatConfig(),
    prometheus=PrometheusConfig(port=8001, prometheus_path="/metrics"),
    open_telemetry=OpenTelemetryConfig(endpoint="http://localhost:4317"),
    frontend=FrontendConfig(host="127.0.0.1", port=5555),
)
```

The web UI reads worker import paths from `CELERY_ROOT_WORKERS` (comma-separated). If you need to override settings before Django settings load:

```python
from celery_root.config import set_settings

set_settings(config)
```

**Beat Scheduler**
To manage schedules from the UI without Django, configure Celery beat to use the Root DB scheduler:

```python
app.conf.beat_scheduler = "celery_root.components.beat.db_scheduler:DatabaseScheduler"
app.conf.beat_db_refresh_seconds = 5.0  # optional polling interval
```

Run one beat per broker/app (Celery beat can only talk to one broker at a time). The UI will read/write schedules in the Root DB.

## Library usage

Start the supervisor from Python:

```python
from celery_root import CeleryRoot

root = CeleryRoot("your_app.celery:app")
root.run()
```

Provide a logger if you want Celery Root to use your logging setup (subprocess logs are forwarded via a queue):

```python
import logging

from celery_root import CeleryRoot

logger = logging.getLogger("celery_root")
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())

root = CeleryRoot("your_app.celery:app", logger=logger)
root.run()
```

## MCP server (AI tools)

Celery Root ships with an optional MCP server that exposes read-only tools over HTTP. It is designed for MCP clients (Codex CLI, Claude Code, etc.) to inspect the Celery Root store safely without write access.


Configuration:

- `CELERY_ROOT_MCP_ENABLED`: Enable the MCP server (`1`/`true`).
- `CELERY_ROOT_MCP_HOST`: Host interface (default: `127.0.0.1`).
- `CELERY_ROOT_MCP_PORT`: Port (default: `9100`).
- `CELERY_ROOT_MCP_PATH`: Base path (default: `/mcp/`).
- `CELERY_ROOT_MCP_AUTH_KEY`: Required auth token for clients.
- `CELERY_ROOT_MCP_READONLY_DB_URL`: Deprecated (RPC-based access replaces direct DB reads).

Example:

```bash
export CELERY_ROOT_MCP_ENABLED=1
export CELERY_ROOT_MCP_AUTH_KEY="your-secret-token"
```

Tools:

- `fetch_schema`: database schema (tables + columns).
- `db_info`: backend metadata.
- `db_query`: read-only SQL access to Celery Root tables (`tasks`, `task_events`,
  `task_relations`, `workers`, `worker_events`, `broker_queue_events`, `schedules`,
  `schema_version`).
- `stats`: dashboard metrics plus task runtime aggregates.

Resources:

- `resource://celery-root/health`: MCP health payload.
- `resource://celery-root/db-catalog`: table catalog and example queries for `db_query`.

Start the supervisor (or MCP server) and open the Settings page to copy client snippets.

## Development

Run checks locally:

```bash
uv run precommit
uv run mypy
uv run pytest
```

## Project structure

- `celery_root/components/`: optional components (web, metrics, beat).
- `celery_root/core/`: engine + DB + logging internals.
- `demo/`: demo workers and task scripts.
- `tests/`: unit and integration tests.
