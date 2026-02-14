<!--
SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
SPDX-FileCopyrightText: 2026 Maximilian Dolling
SPDX-FileContributor: AUTHORS.md

SPDX-License-Identifier: BSD-3-Clause
-->

Goal

Implement a single-writer DB Manager process that is the only component allowed to touch the database driver/storage. All other components (event listeners, web API, OTEL/MCP/Prometheus exporters, etc.) must interact with it through a typed RPC interface using Pydantic schemas.

Key outcomes:

Strict separation: storage logic stays inside DB Manager

All data crossing the process boundary uses Pydantic models (no storage types leak out)

IPC is request/response RPC, supports concurrency, is robust, and is observable

DB selection is driven by int config, but the rest of the system is DB-agnostic

Architecture
Processes

DB Manager process

Owns DB connection(s)

Executes all reads/writes

Knows how to load the correct DB backend based on an int config

Exposes a stable RPC API (method names + request/response schemas)

Clients (other processes/threads)

Event listeners: send events as “write” RPC calls

Web API / OTEL / MCP / Prometheus: send “read” RPC calls

Clients never import or call DB backend code directly

Data contracts (Pydantic-only boundary)
Rule

Every RPC request and response payload MUST be a Pydantic model (or a container of them), and the model definitions MUST live in a shared module that is DB-agnostic.

Examples of shared models:

EventIn, EventStored, Worker, WorkerStatus, MetricPoint, QueryFilters, Page[T], Ok, RpcError

Versioning

Add schema_version: int = 1 to every top-level RPC request model OR to the RPC envelope. This allows evolving schemas without breaking old clients.

RPC transport & serialization
Transport recommendation (in-process, multiprocessing)

Use a multiprocessing-safe bidirectional connection with request/response semantics:

multiprocessing.connection.Listener/Client (socket/pipe-based)

DB Manager runs a Listener, clients connect via Client

Each request gets a request_id, and DB Manager replies with matching request_id

This avoids reinventing low-level socket plumbing and works well in a single host setup.

Serialization recommendation

Use one of these two options:

Option A (simplest, good enough): JSON

Pydantic v2: model_dump_json() / TypeAdapter.validate_json()

Pros: easy debugging, inspectable, fewer dependencies

Cons: slower, larger payloads

Option B (preferred for performance): MessagePack

Use msgpack with Pydantic model_dump(mode="json") then pack, or write a small helper for bytes

Pros: smaller/faster, good for high event throughput

Cons: less human-readable; must standardize encoding of datetimes/bytes

Decision rule

If event throughput is modest: start with JSON

If you expect heavy event volume: implement MessagePack early

Hard requirement
Pick one canonical encoding for the wire and enforce it (no “sometimes JSON, sometimes msgpack”).

RPC envelope (mandatory)

All messages across the wire MUST be wrapped in an envelope. Do not send “raw” models without a header.

Envelope fields

request_id: str (uuid4)

op: str (operation name, e.g. "events.ingest", "workers.get")

payload: dict (Pydantic-dumped form, or bytes if you’re packing nested)

schema_version: int

timestamp: datetime (optional but useful)

client: str (optional: component name)

Response envelope

request_id: str

ok: bool

payload: dict | None

error: {code, message, details}?

Important: errors must be part of the response protocol, not thrown across the boundary.

Operation registry (the RPC “API surface”)

Define a stable set of operations with explicit request and response models.

Example API (adapt to your domain):

Writes

events.ingest

Request: IngestEventRequest(event: EventIn)

Response: IngestEventResponse(stored: EventStored)

workers.upsert

Request: UpsertWorkerRequest(worker: Worker)

Response: UpsertWorkerResponse(worker: Worker)

Reads

workers.get

Request: GetWorkerRequest(worker_id: str)

Response: GetWorkerResponse(worker: Worker | None)

events.query

Request: QueryEventsRequest(filters: QueryFilters, page: PageRequest)

Response: QueryEventsResponse(results: list[EventStored], page: PageInfo)

Health/admin

db.ping

Request: PingRequest()

Response: PingResponse(status: Literal["ok"])

db.stats

Request: StatsRequest()

Response: StatsResponse(...)

Rules

No “generic dict” operations.

No “freeform SQL” operations from clients.

Each operation has typed models and is registered in one place.

DB Manager: backend selection (int config)
Requirement

DB Manager must select its backend using an int config (e.g. db_kind: int), but clients never see backend-specific behavior.

Implementation instructions:

Create a DbBackend protocol / abstract base class with methods corresponding to the RPC ops (or an internal repository interface).

Implement SqliteBackend, PostgresBackend, InMemoryBackend, etc.

DB Manager uses a factory:

db_kind=0 -> InMemoryBackend
db_kind=1 -> SqliteBackend
db_kind=2 -> PostgresBackend
...


Backend MUST only accept/return shared Pydantic models (or internal types that DBM converts before returning).

Concurrency model
DB Manager should be single-writer and deterministic

One DB connection per process (or a small controlled pool inside DBM if your DB supports it safely)

Requests handled sequentially OR with an internal queue and controlled worker threads (only if safe)

Client concurrency

Clients may issue concurrent RPC calls, so:

Either create one RPC connection per calling thread/task

Or implement a client-side lock around a single connection

Instruction: simplest robust approach is one connection per caller (connection pooling can be added later).

Timeouts, retries, and backpressure

Mandatory behaviors:

Every client call supports:

timeout (e.g. default 2–5s for reads, configurable)

max_retries (default 0 for writes unless idempotency is implemented)

DB Manager must apply:

max message size (to prevent memory blowups)

graceful “busy/backpressure” response if queue is overloaded

Idempotency for writes (high value)

If event listeners may resend:

Include idempotency_key: str in write requests (or stable event_id)

DB backend enforces uniqueness

DB Manager can safely retry transient failures

Validation, security, and safety checks

Validate envelopes strictly:

Unknown op → error OP_NOT_FOUND

Schema version mismatch → SCHEMA_UNSUPPORTED

Payload validation error → VALIDATION_ERROR

Authentication:

If using multiprocessing.connection, use authkey

Bind listener to localhost / unix socket only

Observability

DB Manager must log per request:

request_id, op, duration_ms, ok/error_code

optionally payload size
Add tracing hooks:

client attaches trace context fields (if available) into envelope metadata

DB Manager emits spans / metrics

Prometheus exporter / OTEL collector MUST query via read RPC ops (no direct DB access).

File/module layout (suggested)

shared/schemas/

events.py, workers.py, rpc.py (envelope + error models)

dbmanager/

server.py (listener loop, dispatch, lifecycle)

backends/ (sqlite.py, postgres.py, memory.py)

dispatch.py (op registry: op -> handler + request/response types)

client/

rpc_client.py (send request, wait for response, validate, timeouts)

Definition of “done” (acceptance criteria)

✅ All components interact with DB exclusively via RPC client calls

✅ All cross-process payloads are Pydantic models (validated on both ends)

✅ DB backend selection is solely based on int config inside DB Manager

✅ RPC supports errors, timeouts, and structured responses

✅ No backend/storage types leak into other components

✅ Basic db.ping and at least one read + one write op implemented end-to-end
