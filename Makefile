SHELL := /bin/sh

.PHONY: build \
		dist \
		dist_clean \
		install \
		lint \
		publish \
		publish_test

install_frontend:
	npm --prefix frontend/graph-ui install

install_backend:
	uv sync --all-extras --dev --frozen
	uv run pre-commit install

install: install_backend install_frontend

clean:
	rm -rf demo/data/logs

build_frontend: install_frontend
	npm --prefix frontend/graph-ui run build

build: build_frontend

dist: build_frontend
	uv build --no-sources

dist_clean:
	rm -rf dist

lint:
	uv run pre-commit run --all-files

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
