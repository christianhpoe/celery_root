SHELL := /bin/sh

BROKER1_URL ?= amqp://guest:guest@localhost:5672//
BACKEND1_URL ?= redis://localhost:6379/0
BROKER2_URL ?= amqp://guest:guest@localhost:5673//
BACKEND2_URL ?= redis://localhost:6380/0
BROKER3_URL ?= redis://localhost:6381/0
BACKEND3_URL ?= db+postgresql://postgres:postgres@localhost:5432/postgres

CELERY_CNC_DB_PATH ?= celery_cnc.db
CELERY_CNC_WORKERS ?= demo.worker_math:app,demo.worker_text:app,demo.worker_sleep:app
CELERY_CNC_WEB_HOST ?= 127.0.0.1
CELERY_CNC_WEB_PORT ?= 8000

export BROKER_URL
export BACKEND_URL
export CELERY_CNC_DB_PATH
export CELERY_CNC_WORKERS
export CELERY_CNC_WEB_HOST
export CELERY_CNC_WEB_PORT

.PHONY: build \
 		install \
 		lint

clean:
	rm -rf demo/data/logs

FRONTEND_OUT := celery_cnc/web/static/graph/graph.js \
	celery_cnc/web/static/graph/graph.css
FRONTEND_SRC := $(shell find frontend/graph-ui/src -type f)
FRONTEND_DEPS := frontend/graph-ui/package.json \
	frontend/graph-ui/package-lock.json \
	frontend/graph-ui/tsconfig.json \
	frontend/graph-ui/vite.config.ts \
	$(FRONTEND_SRC)

$(FRONTEND_OUT): $(FRONTEND_DEPS)
	npm --prefix frontend/graph-ui run build

build_frontend: $(FRONTEND_OUT)

build: build_frontend

install_frontend:
	npm --prefix frontend/graph-ui install

install_backend:
	uv sync --all-extras --dev --frozen
	uv run pre-commit install

install: install_backend install_frontend

lint:
	CELERY_CNC_WORKERS= uv run pre-commit run --all-files

docker_network:
	docker network create celery_cnc_demo || true

demo_stop_infra:
	docker compose -p celery_cnc_demo -f demo/infra.docker-compose.yml down --volumes --remove-orphans

demo_start_infra: docker_network
	docker compose -p celery_cnc_demo -f demo/infra.docker-compose.yml up -d

demo_worker_math: demo_start_infra
	uv run celery -A demo.worker_math worker -n math@%h -l INFO

demo_worker_text: demo_start_infra
	uv run celery -A demo.worker_text worker -n text@%h -l INFO

demo_worker_sleep: demo_start_infra
	BROKER3_URL=$(BROKER3_URL) BACKEND3_URL=$(BACKEND3_URL) uv run celery -A demo.worker_sleep worker -n sleep@%h -l INFO

demo_workers: demo_start_infra
	docker compose -p celery_cnc_demo -f demo/worker.docker-compose.yml up --build

demo_tasks:
	uv run python demo/schedule_demo_tasks.py

demo_graph_tasks:
	uv run python demo/schedule_demo_tasks.py

demo_cnc: clean build
	uv run python celery_cnc/components/web/manage.py migrate
	uv run python demo/main.py
