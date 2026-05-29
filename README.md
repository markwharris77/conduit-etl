# conduit-etl

A lightweight Python pipeline runtime for data engineering teams who cannot or will not use cloud-managed services.

```
pip install conduit-etl
```

## Why

Most pipeline tools assume you are deploying to a managed cloud service. conduit-etl assumes you are not. It runs on a single VM, an NFS mount, a Cloud Foundry container, or a Kubernetes pod — anywhere Python runs. The catalog is DuckLake (DuckDB + local or S3 storage), the queue is pluggable (memory → SQLite → Postgres), and the executor scales from a single machine to a fleet of stateless HTTP workers.

The CLI is the complete interface. Every operation is a subprocess call, so it works with Control-M, cron, CI pipelines, and shell scripts without any SDK integration.

---

## Quick start

**1. Define a pipeline**

```python
# pipeline.py
import duckdb
from conduit_etl import source, step, sink, Table

@source(schedule="hourly", output="raw_orders")
def raw_orders() -> Table:
    con = duckdb.connect()
    con.execute("ATTACH 'postgresql://...' AS pg (TYPE POSTGRES)")
    return con.sql("SELECT * FROM pg.orders WHERE updated_at > now() - INTERVAL '2 hours'")

@step(output="clean_orders")
def clean_orders(raw_orders: Table) -> Table:
    return raw_orders.filter("amount > 0").select("order_id, amount, customer_id, updated_at")

@sink
def write_parquet(clean_orders: Table) -> None:
    from conduit_etl.sinks.parquet import write_parquet
    write_parquet(clean_orders, "/data/orders/latest.parquet")
```

**2. Run it**

```bash
# One-shot (for cron, Control-M, CI)
conduit run --pipeline pipeline

# Continuous scheduler daemon
conduit scheduler --pipeline pipeline

# Check what happened
conduit status --pipeline pipeline
conduit history
```

---

## How DAG wiring works

Steps are wired automatically from function parameter names. A parameter named `raw_orders` means the step depends on the output table named `raw_orders`. No explicit wiring or decorators needed.

```python
@step
def enrich(clean_orders: Table, customer_dim: Table) -> Table:
    return clean_orders.join(customer_dim, "customer_id")
```

The runtime builds the DAG, resolves execution order (Kahn's algorithm), and runs steps in parallel within each level.

---

## Skip logic

Steps only re-run when something changes. On each tick the runtime computes a fingerprint for each step:

```
fingerprint = {
    input_table: (snapshot_id, row_count),
    ...
    "__fn_hash__": sha256(function source code)
}
```

If the fingerprint matches the last successful run, the step is skipped. A code change (new `__fn_hash__`) forces a re-run even if the input data is identical.

---

## Decorators

### `@source`

No input tables. Pulls data from an external system and writes it to the catalog.

```python
@source(
    schedule = "hourly",      # cron, alias, or interval (e.g. "30m")
    output   = "raw_orders",  # default: function name
    timeout  = "15m",
    retry    = 2,
    tags     = ["finance"],
)
def raw_orders() -> Table:
    ...
```

### `@step`

Reads one or more catalog tables, transforms, writes a new table.

```python
@step(
    schedule      = None,         # run whenever inputs change (default)
    output        = "my_table",   # default: function name
    incremental   = False,        # True → receives only new rows since last run
    merge         = "replace",    # "replace" | "append" | "upsert"
    merge_key     = ["id"],       # required for upsert
    partition_by  = "date",       # fan-out by column (runs max_partitions concurrently)
    max_partitions = 8,
    timeout       = "15m",
    retry         = 2,
    tags          = ["finance"],
)
def clean_orders(raw_orders: Table) -> Table:
    ...
```

### `@sink`

Reads catalog tables, writes to an external system. Return value is ignored.

```python
@sink(schedule="daily", timeout="30m")
def export_to_s3(clean_orders: Table) -> None:
    ...
```

---

## Configuration

Place `pipeline.toml` in the working directory (or pass `--config path/to/config.toml`):

```toml
[catalog]
backend = "local"               # "local" | "s3"
path    = "~/.conduit/catalog"

[queue]
backend = "memory"              # "memory" | "sqlite" | "postgres"
# path  = "~/.conduit/queue.db"    # sqlite
# url   = "${DATABASE_URL}"         # postgres

[executor]
backend = "local"               # "local" | "distributed"
workers = 4
# scheduler_url = "http://host:7700"  # distributed workers point here

[scheduler]
port             = 7700         # job coordination API (distributed mode)
metrics_port     = 7701         # Prometheus metrics + health (always on)
tick             = "10s"
heartbeat_window = "30s"

[steps]
default_timeout  = "15m"
default_retry    = 2
staging_path     = "/tmp/conduit/staging"

[monitoring]
log_level  = "info"
log_format = "json"             # "json" | "text"
```

Environment variable expansion works anywhere: `url = "${DATABASE_URL}"`.

---

## Backends

### Catalog

| Backend | Config | Use case |
|---------|--------|----------|
| `local` | `path = "~/.conduit/catalog"` | Development, single machine |
| `s3` | `url = "s3://bucket/path"` | Production, shared storage (MinIO, Ceph, AWS S3) |

The S3 backend uses DuckDB's built-in `httpfs` extension — no boto3 dependency. Credentials via `key`/`secret` or standard AWS environment variables.

### Queue

| Backend | Config | Use case |
|---------|--------|----------|
| `memory` | — | Default; reconstructed from catalog on restart |
| `sqlite` | `path = "~/.conduit/queue.db"` | Single VM; survives scheduler restart |
| `postgres` | `url = "${DATABASE_URL}"` | HA; multiple scheduler instances, `FOR UPDATE SKIP LOCKED` |

### Executor

| Backend | Config | Use case |
|---------|--------|----------|
| `local` | `workers = 4` | ThreadPoolExecutor; all steps in one process |
| `distributed` | `scheduler_url = "http://host:7700"` | Stateless HTTP workers across any number of machines |

---

## Distributed execution

Start the scheduler (enqueues jobs, serves the coordination API):

```bash
conduit scheduler --pipeline pipeline
```

Start one or more workers on any machine with access to the catalog:

```bash
conduit worker --scheduler http://scheduler-host:7700 --pipeline pipeline
```

Workers poll `/job/next`, execute the step, write results to the shared catalog, and report back. Workers are stateless — restart them freely. The scheduler requeues jobs whose workers go silent (configurable via `heartbeat_window`).

---

## CLI reference

```
conduit run [--steps a,b] [--tag finance]   Run all due steps once and exit
conduit scheduler                           Start the continuous scheduler daemon
conduit worker --scheduler-url URL         Start a worker (distributed mode)

conduit status                              Current step statuses and last run
conduit history [<step>] [--limit 20]      Run history

conduit dag [--format ascii|dot]           Print the pipeline DAG
conduit debug [--at "2024-01-01 12:00"]    DuckDB REPL with catalog loaded

conduit invalidate <step> [--cascade]      Force re-run on next tick
conduit replay <step> [--run <id>]         Re-run a step with its last inputs
conduit backfill <step> --date 2024-01-15  Re-run for a specific date partition

conduit catalog snapshots <table>          List snapshots for a table
conduit catalog diff <table> <s1> <s2>     Row-level diff between two snapshots
conduit catalog gc [--older-than 30d]      Remove old snapshots

All commands accept --config <path> and --output json.
```

---

## Metrics

The scheduler always exposes Prometheus text format at `GET /metrics` on `metrics_port` (default 7701) alongside `GET /health`. Any Prometheus-compatible scraper picks this up — Prometheus, VictoriaMetrics, Grafana Agent, Datadog Agent.

```
conduit_etl_step_runs_total{step="clean_orders",status="success"} 847
conduit_etl_step_duration_seconds_bucket{step="clean_orders",le="10"} 412
conduit_etl_step_rows_out{step="clean_orders"} 141203
conduit_etl_pipeline_lag_seconds{step="clean_orders"} 12.3
conduit_etl_queue_depth 3
```

---

## Connectors

### Sources

**PollSource** — query a database on a schedule with a high-water mark:

```python
from conduit_etl.sources.poll import get_watermark

@source(schedule="hourly", output="raw_orders")
def raw_orders() -> Table:
    since = get_watermark("raw_orders", catalog) or "1970-01-01"
    con = duckdb.connect()
    con.execute("ATTACH 'postgresql://...' AS pg (TYPE POSTGRES)")
    return con.sql(f"SELECT * FROM pg.orders WHERE updated_at > '{since}'")
```

**FileSource** — glob a directory, hash-deduplicate, return only new/changed files:

```python
from conduit_etl.sources.file import file_batch, get_file_hashes

@source(schedule="always", output="raw_csv")
def raw_csv() -> Table:
    prev = get_file_hashes("raw_csv", catalog)
    rel = file_batch("/data/incoming/*.csv", format="csv", previous_hashes=prev)
    # file_batch.new_hashes is available for watermark persistence
    return rel
```

**KafkaSource** — consumer group micro-batch (`pip install conduit-etl[kafka]`):

```python
from conduit_etl.sources.kafka import kafka_batch

@source(schedule="always", output="raw_events")
def raw_events() -> Table:
    return kafka_batch("events", brokers=["kafka:9092"], max_messages=1000)
```

### Sinks

**ParquetSink**:

```python
from conduit_etl.sinks.parquet import write_parquet

@sink
def export(clean_orders: Table) -> None:
    write_parquet(clean_orders, "/data/orders/latest.parquet")
```

**PostgresSink** (`pip install conduit-etl[postgres]`):

```python
from conduit_etl.sinks.postgres import write_postgres

@sink
def to_postgres(clean_orders: Table) -> None:
    write_postgres(clean_orders, conn_str="postgresql://...",
                   table="orders", merge="upsert", merge_key=["order_id"])
```

**KafkaSink** (`pip install conduit-etl[kafka]`):

```python
from conduit_etl.sinks.kafka import write_kafka

@sink
def publish(clean_orders: Table) -> None:
    write_kafka(clean_orders, topic="orders", brokers=["kafka:9092"],
                key_column="order_id")
```

---

## Testing steps

Steps are pure functions over DuckDB relations — unit test them without any runtime:

```python
import duckdb
from pipeline import clean_orders

def test_clean_orders_rejects_negative_amounts():
    raw = duckdb.sql("SELECT 1 AS order_id, -5.0 AS amount, 42 AS customer_id, now() AS updated_at")
    result = clean_orders(raw)
    assert result.fetchdf().empty

def test_clean_orders_passes_positive_amounts():
    raw = duckdb.sql("SELECT 1 AS order_id, 50.0 AS amount, 42 AS customer_id, now() AS updated_at")
    result = clean_orders(raw)
    assert len(result.fetchdf()) == 1
```

For integration tests, use `LocalCatalog` with pytest's `tmp_path` — DuckDB is in-process and fast enough for full pipeline tests.

---

## Installation

```bash
# Core (DuckDB + Click only)
pip install conduit-etl

# With Kafka support
pip install conduit-etl[kafka]

# With PostgreSQL support
pip install conduit-etl[postgres]

# Everything
pip install conduit-etl[all]
```

Requires Python 3.11+.

---

## License

Apache 2.0
