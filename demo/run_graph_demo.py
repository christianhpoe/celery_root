# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Run the demo supervisor and seed tasks for graph previews."""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

_ROOT_CMD: list[str] = [sys.executable, "-m", "demo.main"]
_SEED_CMD: list[str] = [sys.executable, "-m", "demo.schedule_demo_tasks"]
_STARTUP_DELAY_SECONDS = 3.0
_SHUTDOWN_TIMEOUT_SECONDS = 8.0


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _seed_tasks(root: Path) -> None:
    subprocess.run(_SEED_CMD, cwd=root, check=True)  # noqa: S603


def _shutdown(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=_SHUTDOWN_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=_SHUTDOWN_TIMEOUT_SECONDS)


def main() -> None:
    """Start the demo server, seed tasks, and keep running until interrupted."""
    root = _repo_root()
    env = os.environ.copy()
    root_proc = subprocess.Popen(_ROOT_CMD, cwd=root, env=env, text=True)  # noqa: S603
    try:
        time.sleep(_STARTUP_DELAY_SECONDS)
        _seed_tasks(root)
        root_proc.wait()
    except KeyboardInterrupt:
        pass
    finally:
        _shutdown(root_proc)


if __name__ == "__main__":
    main()
