# celery_cnc

Celery Command & Control (CnC) is a multi-worker monitoring, management, and visualization tool for Celery.
It ships with a Django-based UI, a lightweight event listener/collector, and helper utilities for inspecting
queues, tasks, workers, and beat schedules.

## Features

- Task list with filtering, sorting, and details (args/kwargs/result/traceback).
- Task relation graph visualization.
- Worker fleet overview and per-worker drill-down.
- Broker queue inspection and purge actions.
- Beat schedule overview and editor.
- Pluggable storage (SQLite by default).

## Quickstart (demo)

Requirements: Python >= 3.13, `uv`, and Docker (for the demo broker/redis).

```bash
make demo-infra
```

Start the demo workers in separate terminals:

```bash
make demo-worker-math
```

```bash
make demo-worker-text
```

Launch the CnC supervisor + web UI:

```bash
make demo-cnc
```

Then open `http://127.0.0.1:8000`.

To enqueue demo tasks:

```bash
make demo-tasks
```

## Running the web UI

If you already have Celery workers and a broker running, you can point the UI at your apps via
`CELERY_CNC_WORKERS` and run the web server:

```bash
export CELERY_CNC_WORKERS="your_app.celery:app,another_app.celery:app"
uv run python celery_cnc/components/web/manage.py migrate
uv run python -m celery_cnc.components.web.devserver --host 127.0.0.1 --port 8000
```

The UI reads task/worker data from the CnC SQLite store (see configuration below).

## CLI usage

You can run the supervisor via the CLI, either standalone or as a Celery subcommand.

Standalone:

```bash
celery-cnc -A demo.worker_math:app
```

Via Celery:

```bash
celery -A demo.worker_math:app cnc
```

## Configuration

Configuration is explicit via Pydantic models:

```python
from pathlib import Path

from celery_cnc import (
    BeatConfig,
    CeleryCnCConfig,
    DatabaseConfigSqlite,
    FrontendConfig,
    LoggingConfigFile,
    OpenTelemetryConfig,
    PrometheusConfig,
)

config = CeleryCnCConfig(
    logging=LoggingConfigFile(log_dir=Path("./logs")),
    database=DatabaseConfigSqlite(db_path=Path("./celery_cnc.db")),
    beat=BeatConfig(schedule_path=Path("./celerybeat-schedule")),
    prometheus=PrometheusConfig(port=8001, prometheus_path="/metrics"),
    open_telemetry=OpenTelemetryConfig(endpoint="http://localhost:4317"),
    frontend=FrontendConfig(host="127.0.0.1", port=8000),
)
```

Components are enabled when their config is provided (set to `None` to disable).

The web UI still reads worker import paths from `CELERY_CNC_WORKERS` (comma-separated).

If you need to override settings before Django settings load:

```python
from celery_cnc.config import set_settings

set_settings(config)
```

`BROKER_URL` and `BACKEND_URL` are standard Celery settings used by the demo math worker. The demo text and sleep
workers use `BROKER2_URL`/`BACKEND2_URL` and `BROKER3_URL`/`BACKEND3_URL` respectively.

## Library usage

For programmatic use, you can start the supervisor from Python:

```python
from celery_cnc import CeleryCnC

cnc = CeleryCnC("your_app.celery:app")
cnc.run()
```

## MCP server (AI tools)

Celery CnC ships with an optional MCP server that exposes read-only tools over HTTP.
It is designed to let MCP clients (Codex CLI, Claude Code, etc.) inspect the CnC
SQLite store safely without write access.

How it works:

- The MCP server runs as a separate process when `CELERY_CNC_MCP_ENABLED=1`.
- Requests are served from the CnC SQLite store using a read-only SQLAlchemy engine.
- Tools include schema discovery, limited SQL querying (SELECT/WITH only), and a
  dashboard stats payload that matches the web UI.
- Authentication is enforced with a static bearer token (`CELERY_CNC_MCP_AUTH_KEY`).
- The web Settings page renders copy/paste snippets for MCP client configuration
  and CLI commands for Codex + Claude.

Configuration:

- `CELERY_CNC_MCP_ENABLED`: Enable the MCP server (`1`/`true`).
- `CELERY_CNC_MCP_HOST`: Host interface (default: `127.0.0.1`).
- `CELERY_CNC_MCP_PORT`: Port (default: `9100`).
- `CELERY_CNC_MCP_PATH`: Base path (default: `/mcp/`).
- `CELERY_CNC_MCP_AUTH_KEY`: Required auth token for clients.
- `CELERY_CNC_MCP_READONLY_DB_URL`: Optional read-only database URL (defaults to
  SQLite read-only mode using `CELERY_CNC_DB_PATH`). If you provide a regular
  database URL via `CELERY_CNC_MCP_READONLY_DB_URL`, it is used as-is; ensure
  the credentials are truly read-only or queries will not be protected by the
  database itself.

Example:

```bash
export CELERY_CNC_MCP_ENABLED=1
export CELERY_CNC_MCP_AUTH_KEY="your-secret-token"
```

Start the supervisor (or MCP server) and then open the Settings page to grab the
client config snippets. The page includes JSON config for MCP clients plus CLI
examples for Codex and Claude.

## Development

Run checks locally:

```bash
uv run precommit
```

Run tests directly:

```bash
uv run pytest
```

Type checking:

```bash
uv run mypy
```

## Project structure

- `celery_cnc/components/`: optional components (web, metrics, beat).
- `celery_cnc/core/`: engine + DB + logging internals.
- `demo/`: demo workers and task scripts.
- `tests/`: unit and integration tests.
