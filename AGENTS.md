<!--
SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
SPDX-FileCopyrightText: 2026 Maximilian Dolling
SPDX-FileContributor: AUTHORS.md

SPDX-License-Identifier: BSD-3-Clause
-->

# Agent guidelines — Celery Root

Instructions for AI agents (and humans) working in this repo.

---

## Version control

- We use **git**. All changes go through commits; work is tracked in branches/PRs as appropriate.
- Before committing, run the checks below (or rely on pre-commit).

---

## Code quality

### Typing and mypy

- **Fully typed:** All Python code must be type-annotated. Use type hints for function parameters, return types, and module-level names where it helps clarity.
- **mypy (strict):** The codebase is checked with mypy in strict mode. No commits that break `mypy` on the project.

Run type checking:

```bash
uv run mypy
```

(or `mypy src/` / whatever is configured in `pyproject.toml`).

### Rough checks (pre-commit)

We use **pre-commit** to run a consistent set of checks before each commit. Install and run:

```bash
uv add --dev pre-commit
uv run pre-commit install
```

Thereafter, every `git commit` runs the configured hooks (e.g. ruff, mypy, tests). Fix any failures before committing, or temporarily bypass with `git commit --no-verify` only when justified.

Typical hooks to have:

- **ruff** — lint and format (or separate format hook).
- **mypy** — type checking.
- Optional: **pytest** or a fast smoke test so the tree stays green.

Pre-commit config lives in `.pre-commit-config.yaml`. Keep it so that “rough” checks (lint + types + maybe a quick test) pass on commit.

---

## Summary

| Requirement | How |
|-------------|-----|
| Fully typed | Type hints throughout; mypy strict. |
| mypy checked | `uv run mypy` must pass. |
| Rough checks on commit | pre-commit hooks (ruff, mypy, etc.). |
| Git | All changes via git; no bypassing checks without good reason. |
