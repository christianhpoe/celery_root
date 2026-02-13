<!--
SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
SPDX-FileCopyrightText: 2026 Maximilian Dolling
SPDX-FileContributor: AUTHORS.md

SPDX-License-Identifier: BSD-3-Clause
-->

# NEXT_STEPS

## Flower config parity (backwards compatibility)

Source: https://flower.readthedocs.io/en/latest/config.html

Legend: [x] supported, [~] partial, [ ] missing

| Flower option | Meaning (short) | Root status | Current mapping / notes |
| --- | --- | --- | --- |
| address | bind address | [~] | `FrontendConfig.host` in `celery_root/config.py` (defaults differ: Flower empty string vs Root 127.0.0.1) |
| port | bind port | [~] | `FrontendConfig.port` in `celery_root/config.py` (defaults differ: Flower 5555 vs Root 8000) |
| debug | debug mode | [x] | `FrontendConfig.debug` + CLI `--debug/--no-debug` |
| auth | allowlist regex | [x] | `FrontendConfig.auth` via `celery_root/components/web/auth.py` (supports FLOWER_AUTH) |
| basic_auth | basic auth users | [x] | `FrontendConfig.basic_auth` via `celery_root/components/web/auth.py` (supports FLOWER_BASIC_AUTH) |
| auth_provider | oauth provider | [x] | `FrontendConfig.auth_provider` (supports FLOWER_AUTH_PROVIDER) |
| oauth2_key | oauth client id | [x] | `FrontendConfig.oauth2_key` (supports FLOWER_OAUTH2_KEY) |
| oauth2_secret | oauth client secret | [x] | `FrontendConfig.oauth2_secret` (supports FLOWER_OAUTH2_SECRET) |
| oauth2_redirect_uri | oauth redirect | [x] | `FrontendConfig.oauth2_redirect_uri` (supports FLOWER_OAUTH2_REDIRECT_URI) |
| cookie_secret | secure cookie secret | [ ] | No explicit mapping; Django uses `FrontendConfig.secret_key` but not Flower compatible |
| xheaders | trust proxy headers | [ ] | Not implemented in Django middleware stack |
| url_prefix | non-root UI path | [ ] | No URL prefix support in routing/static paths |
| unix_socket | run on unix socket | [ ] | Dev server does not support UNIX sockets |
| certfile | TLS cert | [ ] | No TLS support in dev server/uvicorn |
| keyfile | TLS key | [ ] | No TLS support in dev server/uvicorn |
| ca_certs | TLS CA bundle | [ ] | No TLS support in dev server/uvicorn |
| broker_api | RabbitMQ management API | [ ] | No broker management API integration |
| enable_events | periodic enable_events | [~] | Always enabled in `celery_root/core/event_listener.py`, not configurable |
| inspect_timeout | inspect timeout (ms) | [ ] | Fixed at 1.0s in `celery_root/core/engine/brokers/base.py` |
| auto_refresh | auto-refresh workers | [ ] | JS polling exists but no config flag or env mapping |
| natural_time | relative timestamps | [ ] | UI uses absolute timestamps only |
| tasks_columns | /tasks columns | [ ] | Columns fixed in `celery_root/components/web/templates/tasks/list.html` |
| format_task | custom task formatter | [ ] | No hook for redaction/formatting |
| persistent | persist state | [~] | Always persists to SQLite; no toggle |
| db | state db file | [~] | `DatabaseConfigSqlite.db_path` exists, not Flower-compatible flag |
| state_save_interval | periodic state save | [ ] | DB writes are continuous; no interval control |
| max_workers | memory cap for workers | [ ] | No cap implemented |
| max_tasks | memory cap for tasks | [ ] | No cap implemented |
| purge_offline_workers | auto-prune offline | [ ] | No auto-prune behavior |
| task_runtime_metric_buckets | metrics buckets | [ ] | Buckets are fixed in metrics exporters |
| conf | config file path | [ ] | No `flowerconfig.py` loader or `--conf` |

## Missing compatibility surfaces

- [ ] Load `flowerconfig.py` / `--conf` and map keys into `CeleryRootConfig`.
- [ ] Read all `FLOWER_*` env vars (not just auth) and map to Root settings.
- [ ] CLI flags parity for Flower options (accept `celery flower ...` style flags for the Celery Root entrypoint).

## Feature parity tasks (top priority)

- [ ] Implement `url_prefix` and `xheaders` for reverse proxy deployments.
- [ ] Add TLS support or document proxy termination (`certfile`/`keyfile`/`ca_certs`).
- [ ] Make event enabling configurable (`enable_events`, `inspect_timeout`).
- [ ] Add task list customization (`tasks_columns`, `format_task`, `natural_time`).
- [ ] Add state retention controls (`persistent`, `db`, `state_save_interval`, `max_workers`, `max_tasks`, `purge_offline_workers`).
- [ ] Implement broker management integration (`broker_api`) for queue stats.
- [ ] Make metrics buckets configurable (`task_runtime_metric_buckets`).
