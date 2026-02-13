# Celery Root Demo Workers

This demo spins up three Celery workers with distinct task sets plus RabbitMQ, Redis, and Postgres via Docker Compose.

## Services
- `rabbitmq`: Broker with management UI at http://localhost:15672 (guest/guest).
- `redis`: Result backend.
- `worker_math`: Compute-heavy tasks (chains/chords included).
- `worker_text`: Text-processing tasks.
- `worker_sleep`: Randomized sleep task that returns "hello world" (Redis broker + Postgres backend).
- `seeder`: One-shot container that enqueues ~40 demo jobs to both workers, including chains and a chord.

## Quick start
```bash
cd demo
docker compose up --build
```

Once healthy, the seeder will dispatch tasks. Watch logs for activity:
```bash
docker compose logs -f worker_math worker_text worker_sleep
```

Re-seed manually anytime:
```bash
docker compose run --rm seeder
```

Seed demo tasks only (no server startup):
```bash
make demo_graph_tasks
```

## OpenTelemetry quick check
If you want to validate OTLP metrics locally, run the collector helper:

```bash
cd demo
docker compose -f otel.docker-compose.yml up
```

Then start Celery Root with `OpenTelemetryConfig(endpoint="http://localhost:4317")`.
The collector exposes a Prometheus scrape endpoint at `http://localhost:9464/metrics`
so you can view the OTLP-exported metrics in a browser.

## Local graph preview (non-Docker)
If you want to run the Root web UI + seed demo tasks locally (so you can inspect graphs),
use the helper script from the repo root:

```bash
python demo/run_graph_demo.py
```

This starts the demo Root stack, seeds tasks after a short delay, and keeps running until
you press Ctrl+C.

## Task highlights
- Math worker: multiplication, powers, factorial, Fibonacci, nth roots, prime factors, random walk, moving average, trapezoidal integral, chains (multiply -> power -> log10), chord (Fibonacci fan-out) and more.
- Text worker: word/line/char counts, common words, n-grams, substring search/replace, case transforms, palindrome counting, vowel/consonant stats, deduplication, etc.

## Config
Workers read broker/backend env vars per component:
- Math worker: `BROKER_URL` / `BACKEND_URL`
- Text worker: `BROKER2_URL` / `BACKEND2_URL`
- Sleep worker: `BROKER3_URL` / `BACKEND3_URL`

Adjust queues or concurrency with standard Celery flags, e.g. `-c 4` on the worker commands.
