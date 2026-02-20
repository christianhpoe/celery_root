# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

FROM python:3.13-slim

WORKDIR /app

ARG SETUPTOOLS_SCM_PRETEND_VERSION=0.0.dev0
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SETUPTOOLS_SCM_PRETEND_VERSION=${SETUPTOOLS_SCM_PRETEND_VERSION}

COPY pyproject.toml uv.lock README.md LICENSES /app/
COPY celery_root /app/celery_root
COPY demo /app/demo

RUN mkdir -p celery_root/components/web/static/graph && \
    touch celery_root/components/web/static/graph/graph.js && \
    touch celery_root/components/web/static/graph/graph.css && \
    pip install --no-cache-dir . "celery[redis]" "psycopg2-binary"
