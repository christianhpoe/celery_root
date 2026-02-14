# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""CLI entrypoints for Celery Root."""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

import click
from celery import Celery

from celery_root import CeleryRoot
from celery_root.config import MAX_PORT, CeleryRootConfig, FrontendConfig, get_settings
from celery_root.core.registry import WorkerRegistry

if TYPE_CHECKING:
    from collections.abc import Sequence


def _parse_worker_paths(values: Sequence[str]) -> list[str]:
    paths: list[str] = []
    for value in values:
        for raw in value.split(","):
            cleaned = raw.strip()
            if cleaned:
                paths.append(cleaned)
    return paths


def _resolve_worker_paths(values: Sequence[str]) -> list[str]:
    paths = _parse_worker_paths(values)
    if paths:
        return paths
    env_raw = os.getenv("CELERY_ROOT_WORKERS")
    if not env_raw:
        return []
    return _parse_worker_paths([env_raw])


def _load_apps(paths: Sequence[str]) -> tuple[Celery, ...]:
    if not paths:
        return ()
    try:
        registry = WorkerRegistry(paths)
    except (ImportError, TypeError, ValueError) as exc:
        raise click.UsageError(str(exc)) from exc
    return registry.get_apps()


def _apply_frontend_overrides(
    config: CeleryRootConfig,
    host: str | None,
    port: int | None,
    *,
    debug: bool | None,
) -> CeleryRootConfig:
    if host is None and port is None and debug is None:
        return config
    frontend = config.frontend or FrontendConfig()
    updates: dict[str, object] = {}
    if host is not None:
        updates["host"] = host
    if port is not None:
        updates["port"] = port
    if debug is not None:
        updates["debug"] = debug
    frontend = frontend.model_copy(update=updates)
    return config.model_copy(update={"frontend": frontend})


def _apply_worker_paths(config: CeleryRootConfig, paths: Sequence[str]) -> CeleryRootConfig:
    if not paths:
        return config
    seen: set[str] = set()
    unique_paths: list[str] = []
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        unique_paths.append(path)
    return config.model_copy(update={"worker_import_paths": unique_paths})


def _run_root(apps: Sequence[Celery], config: CeleryRootConfig) -> None:
    if not apps:
        message = "No Celery app configured. Use -A/--app, a worker path argument, or set CELERY_ROOT_WORKERS."
        raise click.UsageError(message)
    root = CeleryRoot(*apps, config=config)
    root.run()


def _get_app_from_context(ctx: click.Context) -> Celery | None:
    obj = ctx.obj
    if obj is None:
        return None
    app = getattr(obj, "app", None)
    return app if isinstance(app, Celery) else None


@click.command(help="Run Celery Root as a standalone service.")
@click.option(
    "-A",
    "--app",
    "apps",
    multiple=True,
    help="Celery app import path (e.g. proj.celery:app). Can be repeated or comma-separated.",
)
@click.option("--host", default=None, help="Bind the web UI to this host.")
@click.option(
    "--port",
    default=None,
    type=click.IntRange(1, MAX_PORT),
    help="Bind the web UI to this port.",
)
@click.option(
    "--debug/--no-debug",
    default=None,
    help="Enable or disable Django debug mode.",
)
@click.argument("workers", nargs=-1)
def main(
    apps: tuple[str, ...],
    host: str | None,
    port: int | None,
    workers: tuple[str, ...],
    *,
    debug: bool | None,
) -> None:
    """Start the Celery Root process manager."""
    paths = _resolve_worker_paths((*apps, *workers))
    config = _apply_frontend_overrides(get_settings(), host, port, debug=debug)
    config = _apply_worker_paths(config, paths)
    loaded_apps = _load_apps(paths)
    _run_root(loaded_apps, config)


@click.command(name="celery_root", help="Run Celery Root using the current Celery app.")
@click.option("--host", default=None, help="Bind the web UI to this host.")
@click.option(
    "--port",
    default=None,
    type=click.IntRange(1, MAX_PORT),
    help="Bind the web UI to this port.",
)
@click.option(
    "--debug/--no-debug",
    default=None,
    help="Enable or disable Django debug mode.",
)
@click.pass_context
def celery_root(ctx: click.Context, host: str | None, port: int | None, *, debug: bool | None) -> None:
    """Entry point for the Celery subcommand integration."""
    config = _apply_frontend_overrides(get_settings(), host, port, debug=debug)
    app = _get_app_from_context(ctx)
    if app is None:
        paths = _resolve_worker_paths(())
        config = _apply_worker_paths(config, paths)
        loaded_apps = _load_apps(paths)
        if not loaded_apps:
            message = "Celery app not provided. Use `celery -A module:app celery_root` or set CELERY_ROOT_WORKERS."
            raise click.UsageError(message)
        _run_root(loaded_apps, config)
        return
    _run_root((app,), config)
