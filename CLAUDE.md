# conduit-etl — project spec and Claude Code handoff

> **Name:** conduit-etl  
> **PyPI target:** `pip install conduit-etl`  
> **CLI entry point:** `conduit`

## What this is

A lightweight Python pipeline runtime for data engineering teams who cannot or will not
use cloud-managed services. It provides:

- Declarative pipelines via Python decorators (`@source`, `@step`, `@sink`)
- DAG inference from function signatures — no explicit wiring
- DuckDB/DuckLake as the execution and catalog layer
- ACID writes with time-travel debugging built in
- Skip logic based on input fingerprints — only re-run when data changes
- Incremental processing with automatic watermark tracking
- Distributed execution via stateless HTTP workers
- A built-in scheduler daemon as the primary operating mode
- A CLI interface for every operation — one-shot mode works with Control-M, cron, CI
- Prometheus text-format metrics — slots into any monitoring stack
- Pluggable backends for queue, catalog, and executor — no deployment assumptions

## Core philosophy

**Simple primitives that compose.** The CLI is the interface because it works everywhere.
Metrics are Prometheus text format because any scraper understands it. Catalog backends
are swappable because some teams have NFS, some have MinIO, some have local disk.
Nothing assumes a specific deployment platform.

**The catalog is the only source of truth.** The scheduler holds no durable state beyond
what is derivable from the catalog. Queue state is reconstructed on restart. This is what
makes the system deployable anywhere, including ephemeral environments like Cloud Foundry.

**Don't reinvent what exists.** Use Hamilton's DAG primitives as inspiration but implement
lightweight. Use dlt as a reference for @source connector patterns. Do not build a custom
query engine — DuckDB is the engine. Do not build a UI — expose metrics and let the user
choose their monitoring stack.

---

## Architecture overview

```
┌─────────────────────────────────────────────────────────┐
│  User code: pipeline/sources/, pipeline/steps/, sinks/  │
│  @source  @step  @sink  decorators                       │
└─────────────────────┬───────────────────────────────────┘
                       │ registers
┌─────────────────────▼───────────────────────────────────┐
│  Runtime                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐               │
│  │   DAG    │  │ Scheduler│  │ Executor │               │
│  │ builder  │  │  loop    │  │ backend  │               │
│  └──────────┘  └──────────┘  └──────────┘               │
└──────────┬──────────────────────────────────────────────┘
           │
    ┌──────▼──────┐     ┌──────────────┐
    │  Queue      │     │   Catalog    │
    │  backend    │     │   backend    │
    │  (pluggable)│     │  (pluggable) │
    └─────────────┘     └──────────────┘
```

### Pluggable backends — the three interfaces everything depends on

Everything concrete is behind one of these three abstract interfaces.
Swapping deployment means swapping a backend, not rewriting application code.

**CatalogBackend** — where data and run history live
- `LocalCatalog` — DuckLake on local filesystem (dev, single machine)
- `S3Catalog` — DuckLake on S3-compatible object store (MinIO, Ceph, AWS S3)

**QueueBackend** — how ready jobs are tracked
- `MemoryQueue` — in-process, reconstructed from catalog on restart (default, CF-safe)
- `SQLiteQueue` — local SQLite file, survives scheduler restart (raw VM)
- `PostgresQueue` — bound Postgres service, supports multiple scheduler instances (HA)

**ExecutorBackend** — how steps are executed
- `LocalExecutor` — ThreadPoolExecutor, multiple threads in one process
- `DistributedExecutor` — HTTP workers, stateless, any number of machines

The config file selects which backend to use. Application code never imports a backend
directly — it only uses the abstract interface.

---

## Project structure

```
conduit_etl/
├── __init__.py
├── cli.py                    # Click CLI — the primary interface
├── config.py                 # TOML loader with env var expansion
│
├── core/
│   ├── decorators.py         # @source, @step, @sink — the public API
│   ├── registry.py           # Global step registry (thread-safe)
│   ├── dag.py                # DAG construction + topological level sort
│   ├── fingerprint.py        # Input fingerprinting + skip logic
│   ├── runtime.py            # Main tick loop + level execution
│   ├── models.py             # Step, Job, Snapshot, RunRecord dataclasses
│   └── errors.py             # Exception hierarchy
│
├── catalog/
│   ├── base.py               # CatalogBackend ABC
│   ├── local.py              # LocalCatalog — DuckLake on filesystem
│   └── s3.py                 # S3Catalog — DuckLake on S3-compat
│
├── queue/
│   ├── base.py               # QueueBackend ABC
│   ├── memory.py             # MemoryQueue — in-process
│   ├── sqlite.py             # SQLiteQueue — local file
│   └── postgres.py           # PostgresQueue — FOR UPDATE SKIP LOCKED
│
├── executor/
│   ├── base.py               # ExecutorBackend ABC
│   ├── local.py              # LocalExecutor — ThreadPoolExecutor
│   └── distributed.py        # DistributedExecutor — HTTP workers
│
├── sources/
│   ├── base.py               # SourceContext ABC
│   ├── kafka.py              # KafkaSource — consumer group + micro-batch
│   ├── poll.py               # PollSource — cron + high-water mark
│   └── file.py               # FileSource — glob/inotify + hash dedup
│
├── sinks/
│   ├── base.py               # SinkContext ABC
│   ├── postgres.py           # PostgresSink — upsert / append
│   ├── parquet.py            # ParquetSink — write to path
│   └── kafka.py              # KafkaSink — publish to topic
│
├── worker/
│   ├── process.py            # Worker process — polls scheduler, executes steps
│   └── server.py             # Scheduler HTTP server — job coordination API
│
└── metrics/
    └── prometheus.py         # Prometheus text format, served at /metrics
```

---

## Abstract interfaces (implement these first, everything else builds on them)

### CatalogBackend

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Iterator
import duckdb

@dataclass
class Snapshot:
    id: str
    table: str
    created_at: datetime
    rows: int
    schema_hash: str
    meta: dict          # step name, input fingerprints, duration, etc.

@dataclass
class CatalogTransaction:
    def write(self, table: str, relation: duckdb.DuckDBPyRelation, meta: dict) -> Snapshot: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

class CatalogBackend(ABC):
    @abstractmethod
    def transaction(self) -> CatalogTransaction: ...

    @abstractmethod
    def latest_snapshot(self, table: str) -> Optional[Snapshot]: ...

    @abstractmethod
    def snapshots_since(self, table: str, since: datetime) -> list[Snapshot]: ...

    @abstractmethod
    def as_relation(self, snapshot: Snapshot) -> duckdb.DuckDBPyRelation:
        """Return a DuckDB relation pointing at this snapshot's data."""
        ...

    @abstractmethod
    def new_rows_since(self, table: str, since_snapshot_id: str) -> duckdb.DuckDBPyRelation:
        """For incremental steps — rows added after a given snapshot."""
        ...

    def run_log(self) -> duckdb.DuckDBPyRelation:
        """Query the run history as a DuckDB relation."""
        ...
```

### QueueBackend

```python
@dataclass
class Job:
    id: str
    step_name: str
    level: int
    input_snapshots: dict[str, str]   # table → snapshot_id
    created_at: datetime
    claimed_by: Optional[str] = None
    started_at: Optional[datetime] = None

class QueueBackend(ABC):
    @abstractmethod
    def enqueue(self, job: Job) -> None: ...

    @abstractmethod
    def claim(self, worker_id: str) -> Optional[Job]:
        """Atomically claim and return one job. Returns None if queue empty."""
        ...

    @abstractmethod
    def heartbeat(self, job_id: str, worker_id: str) -> None: ...

    @abstractmethod
    def complete(self, job_id: str, output_snapshot: Snapshot) -> None: ...

    @abstractmethod
    def fail(self, job_id: str, error: str) -> None: ...

    @abstractmethod
    def requeue_stale(self, heartbeat_window_seconds: int) -> int:
        """Re-queue jobs whose worker has gone silent. Returns count re-queued."""
        ...

    @abstractmethod
    def pending_count(self) -> int: ...
```

### ExecutorBackend

```python
@dataclass
class StepResult:
    step_name: str
    staging_path: str          # Parquet written to staging area
    rows: int
    duration_seconds: float
    schema: dict

class ExecutorBackend(ABC):
    @abstractmethod
    def submit(self, step: Step, input_relations: dict[str, duckdb.DuckDBPyRelation]) -> Future[StepResult]:
        """Execute a step. Does NOT write to catalog — returns staging result."""
        ...

    @abstractmethod
    def shutdown(self, wait: bool = True) -> None: ...

    @property
    @abstractmethod
    def active_count(self) -> int: ...
```

---

## Core algorithms

### DAG construction

```python
def build_dag(steps: list[Step]) -> dict[str, list[str]]:
    """
    Returns adjacency list: step_name → list of step_names that depend on it.
    Inferred purely from parameter names matching output table names.
    """
    producers: dict[str, str] = {s.output_name: s.name for s in steps}
    graph: dict[str, list[str]] = {s.name: [] for s in steps}
    for step in steps:
        for input_name in step.input_names:
            if input_name in producers:
                producer = producers[input_name]
                graph[producer].append(step.name)
    return graph

def topological_levels(graph: dict[str, list[str]]) -> list[list[str]]:
    """
    Kahn's algorithm. Returns steps grouped by level.
    Steps in the same level have no dependency between them — safe to run concurrently.
    """
    ...
```

### Fingerprint and skip logic

```python
def compute_fingerprint(step: Step, catalog: CatalogBackend) -> dict:
    """
    fingerprint = {
        input_table: (snapshot_id, row_count),
        ...
        "__fn_hash__": sha256 of function source code
    }
    If fingerprint matches last run's stored fingerprint → skip.
    fn_hash mismatch means code changed → re-run even if data unchanged.
    """
    ...
```

### Runtime tick

```python
def tick(self):
    dag = build_dag(self.registry.all_steps())
    levels = topological_levels(dag)

    for level_steps in levels:
        ready = []
        for step_name in level_steps:
            step = self.registry.get(step_name)
            if not step.schedule.is_due():
                continue
            if not self._inputs_available(step):
                continue
            fp = compute_fingerprint(step, self.catalog)
            if not self._fingerprint_changed(step, fp):
                continue          # skip — nothing changed
            ready.append((step, fp))

        if not ready:
            continue

        # Submit all ready steps in this level concurrently
        futures = {
            self.executor.submit(step, self._resolve_inputs(step)): (step, fp)
            for step, fp in ready
        }

        # Collect results — serialize catalog commits
        for future in as_completed(futures):
            step, fp = futures[future]
            result = future.result()
            with self.catalog.transaction() as txn:
                snapshot = txn.write(step.output_name, result.staging_path, {
                    "step": step.name,
                    "input_fingerprint": fp,
                    "rows": result.rows,
                    "duration_seconds": result.duration_seconds,
                })
                txn.commit()
            self._record_run(step, fp, snapshot)
```

---

## CLI commands

The CLI is the complete interface. Every operation is a subprocess call.

The **primary mode is the built-in scheduler daemon** — it runs continuously, ticks every
N seconds, detects ready steps, dispatches them to workers, and manages the full pipeline
lifecycle automatically. You start it once and it runs your pipeline indefinitely.

`conduit run` (one-shot mode) is a secondary interface for cases where an external
scheduler already owns the trigger — Control-M, cron, CI pipelines. It executes all
due steps once and exits cleanly, so the external tool can see the exit code and manage
retries. Both modes use the same runtime and produce the same catalog output.

```
# Primary: built-in scheduler daemon (the normal operating mode)
conduit scheduler          Start the continuous scheduler — ticks, dispatches, monitors
conduit worker             Start a worker process (connect to scheduler, pull jobs)

# Secondary: one-shot execution (for external schedulers like Control-M or cron)
conduit run                Run all due steps once, exit when pipeline completes
conduit run --steps a,b    Run specific steps and their dependencies
conduit run --tag finance  Run steps matching a tag

# Inspection
conduit status             Show current step statuses (last run, next due, etc.)
conduit history            Show run log (tabular, last 20 runs per step)
conduit history <step>     Show run history for one step
conduit dag                Print the DAG as ASCII or DOT format

# Debugging
conduit debug              Drop into DuckDB REPL with latest catalog state
conduit debug --at <ts>    REPL with catalog state at a point in time
conduit replay <step>      Re-run a step locally with its last inputs (for debugging)
conduit replay <step> --run <id>   Re-run with inputs from a specific run

# Control
conduit invalidate <step>           Force re-run on next tick
conduit invalidate <step> --cascade Also invalidate all downstream steps
conduit backfill <step> --date <d>  Re-run for a specific date partition

# Catalog
conduit catalog snapshots <table>   List snapshots for a table
conduit catalog diff <table> <snap1> <snap2>   Diff two snapshots
conduit catalog gc --older-than 30d  Remove old snapshots
```

All commands respect `--config <path>` (default: `./pipeline.toml`) and
`--output json` for machine-readable output.

---

## Metrics (Prometheus text format)

Served at `GET /metrics` on the scheduler's port. Any Prometheus-compatible
scraper (Prometheus, VictoriaMetrics, Grafana Agent, Datadog Agent) picks this up.
No SDK dependency — write raw text format.

```
# HELP conduit_etl_step_runs_total Total step executions
# TYPE conduit_etl_step_runs_total counter
conduit_etl_step_runs_total{step="clean_orders",status="success"} 847
conduit_etl_step_runs_total{step="clean_orders",status="failed"} 3
conduit_etl_step_runs_total{step="clean_orders",status="skipped"} 201

# HELP conduit_etl_step_duration_seconds Step execution duration
# TYPE conduit_etl_step_duration_seconds histogram
conduit_etl_step_duration_seconds_bucket{step="clean_orders",le="1"} 0
conduit_etl_step_duration_seconds_bucket{step="clean_orders",le="10"} 412
conduit_etl_step_duration_seconds_bucket{step="clean_orders",le="60"} 847
conduit_etl_step_duration_seconds_sum{step="clean_orders"} 9823.4
conduit_etl_step_duration_seconds_count{step="clean_orders"} 847

# HELP conduit_etl_step_rows_out Rows written per step execution
# TYPE conduit_etl_step_rows_out gauge
conduit_etl_step_rows_out{step="clean_orders"} 141203

# HELP conduit_etl_pipeline_lag_seconds Seconds behind schedule (key health signal)
# TYPE conduit_etl_pipeline_lag_seconds gauge
conduit_etl_pipeline_lag_seconds{step="clean_orders"} 12.3

# HELP conduit_etl_worker_active Currently active workers
# TYPE conduit_etl_worker_active gauge
conduit_etl_worker_active{worker="w1"} 1

# HELP conduit_etl_catalog_snapshots_total Total snapshots in catalog
# TYPE conduit_etl_catalog_snapshots_total counter
conduit_etl_catalog_snapshots_total 2341

# HELP conduit_etl_queue_depth Current jobs waiting in queue
# TYPE conduit_etl_queue_depth gauge
conduit_etl_queue_depth 3
```

Also expose `GET /health` returning `{"status": "ok", "workers": N, "queue_depth": N}`.
Standard enough for CF health checks, Kubernetes liveness probes, load balancers, anything.

---

## Configuration file (pipeline.toml)

```toml
[catalog]
backend  = "local"                          # "local" | "s3"
path     = "~/.conduit/catalog"          # local backend
# url    = "s3://bucket/path"              # s3 backend
# endpoint = "http://minio.internal:9000"  # s3 backend (MinIO)
# key    = "${MINIO_KEY}"                  # env var expansion
# secret = "${MINIO_SECRET}"

[queue]
backend  = "memory"                         # "memory" | "sqlite" | "postgres"
# path   = "~/.conduit/queue.db"         # sqlite backend
# url    = "${DATABASE_URL}"               # postgres backend

[executor]
backend  = "local"                          # "local" | "distributed"
workers  = 4                                # threads (local) or max concurrent (distributed)
# scheduler_url = "http://host:7700"       # distributed backend (worker side)

[scheduler]
port             = 7700
metrics_port     = 7701
tick             = "10s"
heartbeat_window = "30s"

[steps]
default_timeout = "15m"
default_retry   = 2
staging_path    = "/tmp/conduit/staging"

# Named connections — referenced by @source / @sink by name, not URL
[connections.my_db]
type = "postgres"
url  = "${DATABASE_URL}"
pool = 4

[connections.kafka]
type    = "kafka"
brokers = ["kafka-01:9092", "kafka-02:9092"]

[monitoring]
log_level  = "info"
log_format = "json"           # "json" | "text" — json for log aggregators
```

Env var expansion with `${VAR}` works anywhere in the file.
`conduit run` without `--config` looks for `pipeline.toml` in CWD, then `~/.conduit/pipeline.toml`.

---

## @step decorator full signature

```python
@step(
    # Scheduling
    schedule = "hourly",          # cron string or alias: "minutely", "hourly", "daily"
                                  # omit for steps that run whenever inputs change

    # Incremental processing
    incremental = False,          # True → step receives only new rows since last run
    merge       = "replace",      # "replace" | "append" | "upsert"
    merge_key   = None,           # column(s) for upsert merge

    # Partitioning
    partition_by = None,          # column to fan-out on (e.g. "date")
    max_partitions = 8,           # max concurrent partition workers

    # Reliability
    timeout = "15m",
    retry   = 2,
    retry_on = (Exception,),      # which exceptions trigger retry

    # Organisation
    tags    = [],                 # for selective runs: conduit run --tag finance
    description = "",
)
def my_step(input_table: Table, another_table: Table) -> Table:
    ...
```

The `Table` type is a thin wrapper around `duckdb.DuckDBPyRelation`.
Steps can return `Table` (single output) or `dict[str, Table]` (multiple named outputs).

---

## Scheduler HTTP API (worker coordination only)

Four endpoints. This is not a user-facing API — it's internal worker coordination.
Workers and scheduler are the only consumers.

```
GET  /health
     → {"status":"ok","workers":3,"queue_depth":2,"tick_count":847}

GET  /job/next?worker_id=w1
     → Job JSON or 204 No Content if queue empty

POST /job/{id}/heartbeat
     body: {"worker_id":"w1"}
     → 200 OK

POST /job/{id}/done
     body: {"worker_id":"w1","staging_path":"/tmp/.../result.parquet","rows":141203,"duration":12.3}
     → 200 OK

POST /job/{id}/failed
     body: {"worker_id":"w1","error":"...traceback...","retry":true}
     → 200 OK
```

Use Python's built-in `http.server` or `wsgiref` — no web framework dependency.
This keeps the dependency footprint minimal and deployable everywhere.

---

## Phase 1 — MVP (build this first)

Goal: a working single-machine pipeline with local catalog and in-memory queue.
Everything else is additive. Ship something runnable.

### Must have in phase 1

- [ ] `@step` decorator — basic, no incremental, no partitioning
- [ ] `@source` decorator — PollSource only (DB polling with watermark)
- [ ] `@sink` decorator — ParquetSink only (write to path)
- [ ] DAG inference from type annotations
- [ ] Topological level sort
- [ ] Fingerprint + skip logic (including fn_hash)
- [ ] LocalCatalog backend (DuckLake on local filesystem)
- [ ] MemoryQueue backend
- [ ] LocalExecutor backend (ThreadPoolExecutor)
- [ ] `conduit scheduler` CLI command — built-in daemon (primary operating mode)
- [ ] `conduit run` CLI command — one-shot for external schedulers (Control-M, cron, CI)
- [ ] `conduit status` CLI command
- [ ] `conduit history` CLI command
- [ ] `conduit debug` CLI command (DuckDB REPL with catalog state)
- [ ] `pipeline.toml` config loading with env var expansion
- [ ] Prometheus text metrics written to stdout or file (no HTTP server yet)
- [ ] Structured JSON logging

### Explicitly out of scope for phase 1

- Kafka source/sink
- S3 catalog backend
- SQLite / Postgres queue backends
- Distributed executor / HTTP worker API
- `conduit scheduler` daemon mode
- `conduit worker` process
- Incremental mode
- Partitioned execution
- Backfill / invalidate commands
- `conduit replay`
- HTTP metrics server

Phase 1 is: write a pipeline in Python, run `conduit run`, it executes the DAG,
skips unchanged steps, writes outputs, shows you what happened. That's the core loop.

---

## Phase 2 — distributed execution

- [ ] Scheduler HTTP server (`conduit scheduler` daemon)
- [ ] Worker process (`conduit worker`)
- [ ] HTTP metrics server at `/metrics` and `/health`
- [ ] SQLiteQueue backend
- [ ] DistributedExecutor backend
- [ ] KafkaSource connector
- [ ] PostgresSink connector
- [ ] `conduit invalidate` command
- [ ] Incremental mode (`incremental=True` on @step)

## Phase 3 — production hardening

- [ ] S3Catalog backend (MinIO / Ceph / AWS S3)
- [ ] PostgresQueue backend (HA scheduler)
- [ ] `conduit replay` command
- [ ] `conduit backfill` command
- [ ] `conduit catalog gc` command
- [ ] `conduit dag` ASCII/DOT output
- [ ] Partition fan-out (`partition_by` on @step)
- [ ] KafkaSink connector
- [ ] FileSource connector
- [ ] Schema evolution handling in catalog
- [ ] Dead-letter table for failed records

---

## What to avoid building

These would make the project harder to maintain and less portable:

- **No web UI** — expose metrics, let users choose Grafana/Datadog/whatever
- **No ORM** — DuckDB SQL strings are fine, keep it direct
- **No async** — threading is simpler, debuggable, and sufficient; asyncio adds complexity for no gain here
- **No custom serialisation** — Parquet for data, JSON for metadata, TOML for config
- **No plugin system yet** — keep the backend interfaces internal until the API is stable
- **No cloud-provider SDKs at the core** — S3 access goes through DuckDB's httpfs, not boto3

---

## Key dependencies (keep this list short)

```toml
[project]
dependencies = [
    "duckdb>=1.2.0",          # query engine + catalog
    "click>=8.0",             # CLI
    "tomllib",                # config (stdlib in Python 3.11+, backport for 3.10)
    "confluent-kafka",        # Kafka source/sink (optional, phase 2)
    "psycopg[binary]",        # Postgres sink + queue backend (optional)
    "python-dateutil",        # schedule parsing
]
```

DuckDB ships with httpfs for S3. No boto3, no azure-storage, no GCS client.
All optional dependencies should be behind `extras_require` — the core must install
with only `duckdb` and `click`.

---

## Testing approach

Steps are pure functions — unit test them without any runtime infrastructure:

```python
import duckdb
from pipeline.steps.clean import clean_orders

def test_clean_orders_filters_negative_amounts():
    raw = duckdb.sql("SELECT 1 AS order_id, -5.0 AS amount")
    result = clean_orders.__wrapped__(raw)   # bypass decorator
    assert result.fetchdf().empty

def test_clean_orders_passes_positive_amounts():
    raw = duckdb.sql("SELECT 1 AS order_id, 50.0 AS amount")
    result = clean_orders.__wrapped__(raw)
    assert len(result.fetchdf()) == 1
```

For integration tests, use a `LocalCatalog` pointing at a `tmp_path` fixture.
No mocking needed — DuckDB is in-process and fast enough for test suites.

---

## Handoff notes for Claude Code

Start with `conduit/core/` and work outward. The order matters:

1. `models.py` — dataclasses with no dependencies
2. `errors.py` — exception hierarchy
3. `catalog/base.py` + `catalog/local.py` — foundation everything builds on
4. `queue/base.py` + `queue/memory.py` — simplest queue
5. `executor/base.py` + `executor/local.py` — thread executor
6. `core/decorators.py` + `core/registry.py` — @step, @source, @sink
7. `core/dag.py` — DAG construction and level sort
8. `core/fingerprint.py` — skip logic
9. `core/runtime.py` — the tick loop, wires everything together
10. `cli.py` — Click commands wrapping the runtime
11. `config.py` — TOML loading
12. `metrics/prometheus.py` — text format metrics
13. `sources/poll.py` — first real source connector
14. `sinks/parquet.py` — first real sink connector
15. Tests throughout, not at the end

At each step: write the interface first, then the implementation, then the test.
The interfaces are defined above — do not deviate from them in phase 1 or switching
backends later becomes painful.

Python version target: 3.11+. Use `tomllib` from stdlib, `match` statements where
they improve clarity, `dataclasses` throughout, type annotations everywhere.