"""conduit CLI — the complete interface for the pipeline runtime.

All commands respect ``--config`` (default: pipeline.toml) and
``--output json`` for machine-readable output.
"""

from __future__ import annotations

import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any

import click

from conduit_etl.config import PipelineConfig, load as load_config
from conduit_etl.core.errors import ConduitError


# --------------------------------------------------------------------------- #
# Logging setup
# --------------------------------------------------------------------------- #

def _setup_logging(cfg: PipelineConfig) -> None:
    level = getattr(logging, cfg.monitoring.log_level.upper(), logging.INFO)
    if cfg.monitoring.log_format == "json":
        fmt = '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}'
    else:
        fmt = "%(asctime)s %(levelname)-8s %(name)s: %(message)s"
    logging.basicConfig(level=level, format=fmt, stream=sys.stderr)


# --------------------------------------------------------------------------- #
# Backend factory helpers
# --------------------------------------------------------------------------- #

def _make_catalog(cfg: PipelineConfig):
    if cfg.catalog.backend == "s3":
        from conduit_etl.catalog.s3 import S3Catalog
        return S3Catalog(
            cfg.catalog.url,
            endpoint=cfg.catalog.endpoint,
            key=cfg.catalog.key,
            secret=cfg.catalog.secret,
            region=cfg.catalog.region,
        )
    from conduit_etl.catalog.local import LocalCatalog
    return LocalCatalog(cfg.catalog.path)


def _make_queue(cfg: PipelineConfig):
    backend = cfg.queue.backend
    if backend == "sqlite":
        from conduit_etl.queue.sqlite import SQLiteQueue
        return SQLiteQueue(cfg.queue.path)
    if backend == "postgres":
        from conduit_etl.queue.postgres import PostgresQueue
        return PostgresQueue(cfg.queue.url)
    from conduit_etl.queue.memory import MemoryQueue
    return MemoryQueue()


def _make_executor(cfg: PipelineConfig, queue=None):
    backend = cfg.executor.backend
    staging = cfg.executor.staging_path or cfg.steps.staging_path
    if backend == "distributed":
        from conduit_etl.executor.distributed import DistributedExecutor
        return DistributedExecutor(queue=queue, staging_path=staging)
    from conduit_etl.executor.local import LocalExecutor
    return LocalExecutor(workers=cfg.executor.workers, staging_path=staging)


def _make_runtime(cfg: PipelineConfig, *, tags=None, steps=None, tick_interval=None):
    from conduit_etl.core.registry import get_registry
    from conduit_etl.core.runtime import Runtime
    from conduit_etl.core.models import parse_duration

    catalog = _make_catalog(cfg)
    queue = _make_queue(cfg)
    executor = _make_executor(cfg, queue=queue)
    registry = get_registry()

    interval = tick_interval
    if interval is None:
        interval = parse_duration(cfg.scheduler.tick).total_seconds()

    heartbeat_window = parse_duration(cfg.scheduler.heartbeat_window).total_seconds()

    return Runtime(
        catalog=catalog,
        queue=queue,
        executor=executor,
        registry=registry,
        tick_interval=interval,
        heartbeat_window=heartbeat_window,
        tags=tags,
        step_names=steps,
    ), catalog, queue, executor


# --------------------------------------------------------------------------- #
# Output helpers
# --------------------------------------------------------------------------- #

def _emit(obj: Any, output_fmt: str) -> None:
    if output_fmt == "json":
        click.echo(json.dumps(obj, default=str, indent=2))
    else:
        if isinstance(obj, list):
            for row in obj:
                click.echo(row)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                click.echo(f"{k}: {v}")
        else:
            click.echo(str(obj))


# --------------------------------------------------------------------------- #
# Root group
# --------------------------------------------------------------------------- #

@click.group()
@click.option("--config", "-c", default=None, help="Path to pipeline.toml")
@click.option("--output", "-o", default="text", type=click.Choice(["text", "json"]), help="Output format")
@click.pass_context
def main(ctx: click.Context, config: str | None, output: str) -> None:
    """conduit — lightweight Python pipeline runtime."""
    ctx.ensure_object(dict)
    ctx.obj["output"] = output
    try:
        cfg = load_config(config)
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc
    ctx.obj["cfg"] = cfg
    _setup_logging(cfg)


# --------------------------------------------------------------------------- #
# conduit run
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--steps", default=None, help="Comma-separated step names to run")
@click.option("--tag", default=None, help="Only run steps with this tag")
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.pass_context
def run(ctx: click.Context, steps: str | None, tag: str | None, pipeline: tuple[str, ...]) -> None:
    """Run all due steps once and exit."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    step_names = [s.strip() for s in steps.split(",")] if steps else None
    tags = [tag] if tag else None

    try:
        runtime, catalog, _queue, _executor = _make_runtime(cfg, tags=tags, steps=step_names)
        results = runtime.run_once()
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    _emit(results, output)

    failed = [n for n, s in results.items() if s == "failed"]
    if failed:
        raise click.ClickException(f"steps failed: {', '.join(failed)}")


# --------------------------------------------------------------------------- #
# conduit scheduler
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.option("--tick", default=None, help="Tick interval override (e.g. 30s)")
@click.option("--port", default=None, type=int, help="HTTP API port (default: from config)")
@click.pass_context
def scheduler(ctx: click.Context, pipeline: tuple[str, ...], tick: str | None, port: int | None) -> None:
    """Start the continuous scheduler daemon with HTTP API."""
    cfg: PipelineConfig = ctx.obj["cfg"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    tick_interval = None
    if tick:
        from conduit_etl.core.models import parse_duration
        tick_interval = parse_duration(tick).total_seconds()

    try:
        runtime, catalog, queue, executor = _make_runtime(cfg, tick_interval=tick_interval)

        from conduit_etl.metrics.prometheus import MetricsRegistry
        from conduit_etl.worker.metrics_server import MetricsServer
        metrics = MetricsRegistry()

        # Always start the metrics/health server.
        metrics_srv = MetricsServer(queue=queue, metrics=metrics, executor=executor)
        metrics_srv.start(port=cfg.scheduler.metrics_port)

        # Start the job-coordination API only when using the distributed executor.
        from conduit_etl.executor.distributed import DistributedExecutor
        job_srv = None
        if isinstance(executor, DistributedExecutor):
            from conduit_etl.worker.server import SchedulerServer
            job_srv = SchedulerServer(queue=queue, executor=executor, metrics=metrics)
            job_srv.start(port=port or cfg.scheduler.port)

        def _on_tick():
            metrics_srv.increment_tick()
            if job_srv:
                job_srv.increment_tick()

        runtime.run_forever(on_tick=_on_tick)

        metrics_srv.stop()
        if job_srv:
            job_srv.stop()
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc


# --------------------------------------------------------------------------- #
# conduit worker
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--scheduler-url", default=None, help="Scheduler HTTP URL (e.g. http://host:7700)")
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.option("--poll-interval", default=1.0, type=float, help="Seconds between job polls")
@click.pass_context
def worker(ctx: click.Context, scheduler_url: str | None, pipeline: tuple[str, ...], poll_interval: float) -> None:
    """Start a worker process that polls the scheduler for jobs."""
    cfg: PipelineConfig = ctx.obj["cfg"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    url = scheduler_url or cfg.executor.scheduler_url
    if not url:
        raise click.ClickException(
            "scheduler URL required: pass --scheduler-url or set executor.scheduler_url in config"
        )

    from conduit_etl.core.registry import get_registry
    from conduit_etl.worker.process import WorkerProcess

    staging = cfg.executor.staging_path or cfg.steps.staging_path
    catalog = _make_catalog(cfg)
    registry = get_registry()

    wp = WorkerProcess(
        scheduler_url=url,
        registry=registry,
        catalog=catalog,
        staging_path=staging,
        poll_interval=poll_interval,
    )
    try:
        wp.run()
    finally:
        catalog.close()


# --------------------------------------------------------------------------- #
# conduit status
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.pass_context
def status(ctx: click.Context, pipeline: tuple[str, ...]) -> None:
    """Show current step statuses."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    from conduit_etl.core.registry import get_registry

    try:
        catalog = _make_catalog(cfg)
        registry = get_registry()
        rows = []
        for step in registry.all_steps():
            last = catalog.last_run(step.name)
            snap = catalog.latest_snapshot(step.output_name)
            rows.append({
                "step": step.name,
                "kind": step.kind.value,
                "schedule": step.schedule.raw or "always",
                "last_status": last.status if last else "never",
                "last_run": str(last.finished_at) if last else "-",
                "last_rows": last.rows if last else 0,
                "snapshot_id": snap.id if snap else "-",
            })
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    if output == "json":
        _emit(rows, output)
    else:
        if not rows:
            click.echo("No steps registered (pass --pipeline to load your pipeline).")
            return
        header = f"{'STEP':<30} {'KIND':<8} {'SCHEDULE':<12} {'STATUS':<10} {'LAST RUN':<22} {'ROWS':>8}"
        click.echo(header)
        click.echo("-" * len(header))
        for r in rows:
            click.echo(
                f"{r['step']:<30} {r['kind']:<8} {r['schedule']:<12} "
                f"{r['last_status']:<10} {r['last_run']:<22} {r['last_rows']:>8}"
            )


# --------------------------------------------------------------------------- #
# conduit history
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("step_name", required=False)
@click.option("--limit", default=20, help="Number of records to show")
@click.pass_context
def history(ctx: click.Context, step_name: str | None, limit: int) -> None:
    """Show run history (all steps or one step)."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    try:
        catalog = _make_catalog(cfg)
        rel = catalog.run_log()
        if step_name:
            rel = rel.filter(f"step_name = '{step_name}'")
        rel = rel.limit(limit)
        rows_raw = rel.fetchall()
        cols = rel.columns
        rows = [dict(zip(cols, r)) for r in rows_raw]
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    if output == "json":
        _emit(rows, output)
    else:
        if not rows:
            click.echo("No run history.")
            return
        for r in rows:
            click.echo(
                f"{r.get('finished_at','-')!s:22}  {r.get('step_name','?'):<30}  "
                f"{r.get('status','?'):<10}  {r.get('rows',0):>8} rows  "
                f"{r.get('duration_seconds',0):.2f}s"
            )


# --------------------------------------------------------------------------- #
# conduit debug
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--at", default=None, help="Catalog state at a timestamp (YYYY-MM-DD HH:MM:SS)")
@click.pass_context
def debug(ctx: click.Context, at: str | None) -> None:
    """Drop into a DuckDB REPL with the latest catalog state."""
    cfg: PipelineConfig = ctx.obj["cfg"]

    try:
        catalog = _make_catalog(cfg)
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    if not hasattr(catalog, "connection"):
        raise click.ClickException(
            "conduit debug requires a catalog with a local DuckDB connection. "
            "LocalCatalog and S3Catalog both support this; other backends do not."
        )

    con = catalog.connection()
    if at:
        click.echo(f"Note: time-travel to {at!r} — use AT (VERSION => N) syntax in queries.")

    from conduit_etl.catalog.local import LocalCatalog
    if isinstance(catalog, LocalCatalog):
        run_table = "runs.run_records"
        dead_table = "runs.dead_letters"
    else:
        run_table = "run_records"
        dead_table = "dead_letters"

    click.echo("conduit debug — DuckDB shell.")
    click.echo("  Tables:       SELECT * FROM lake.<table_name>")
    click.echo(f"  Run log:      SELECT * FROM {run_table} ORDER BY finished_at DESC LIMIT 20")
    click.echo(f"  Dead letters: SELECT * FROM {dead_table}")
    click.echo("  Time-travel:  SELECT * FROM lake.<table> AT (VERSION => N)")
    click.echo("Type .quit or Ctrl-D to exit.\n")

    while True:
        try:
            sql = click.prompt("duckdb", prompt_suffix="> ")
        except (EOFError, click.Abort):
            break
        if sql.strip().lower() in {".quit", ".exit", "quit", "exit"}:
            break
        try:
            result = con.execute(sql)
            if result.description:
                cols = [d[0] for d in result.description]
                click.echo("  ".join(cols))
                click.echo("-" * (sum(len(c) for c in cols) + 2 * len(cols)))
                for row in result.fetchall():
                    click.echo("  ".join(str(v) for v in row))
        except Exception as exc:
            click.echo(f"Error: {exc}", err=True)

    catalog.close()


# --------------------------------------------------------------------------- #
# conduit invalidate
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("step_name")
@click.option("--cascade", is_flag=True, default=False, help="Also invalidate all downstream steps")
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.pass_context
def invalidate(ctx: click.Context, step_name: str, cascade: bool, pipeline: tuple[str, ...]) -> None:
    """Force a step (and optionally its downstream steps) to re-run on next tick.

    Invalidation works by deleting the most recent success run record for the
    step, which causes the fingerprint check to treat it as never-run.
    """
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    try:
        catalog = _make_catalog(cfg)
        invalidated = _do_invalidate(step_name, cascade, catalog)
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    _emit({"invalidated": invalidated}, output)


def _do_invalidate(step_name: str, cascade: bool, catalog) -> list[str]:
    """Delete run records so the step appears as never-run. Returns invalidated names."""
    invalidated = [step_name]

    if cascade:
        try:
            from conduit_etl.core.registry import get_registry
            from conduit_etl.core.dag import build_dag
            steps = get_registry().all_steps()
            if steps:
                dag = build_dag(steps)
                visited: set[str] = {step_name}
                frontier = list(dag.get(step_name, []))
                while frontier:
                    s = frontier.pop(0)
                    if s not in visited:
                        visited.add(s)
                        invalidated.append(s)
                        frontier.extend(dag.get(s, []))
        except Exception:
            pass

    catalog.invalidate_runs(invalidated)
    return invalidated


# --------------------------------------------------------------------------- #
# conduit dag
# --------------------------------------------------------------------------- #

@main.command()
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.option("--format", "fmt", default="ascii", type=click.Choice(["ascii", "dot"]), help="Output format")
@click.pass_context
def dag(ctx: click.Context, pipeline: tuple[str, ...], fmt: str) -> None:
    """Print the pipeline DAG as ASCII or Graphviz DOT."""
    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    from conduit_etl.core.registry import get_registry
    from conduit_etl.core.dag import build_dag, execution_order

    steps = get_registry().all_steps()
    if not steps:
        click.echo("No steps registered (pass --pipeline to load your pipeline).")
        return

    graph = build_dag(steps)
    by_name = {s.name: s for s in steps}

    if fmt == "dot":
        lines = ["digraph conduit {", '  rankdir=LR;', '  node [shape=box];']
        for step in steps:
            kind = by_name[step.name].kind.value
            lines.append(f'  "{step.name}" [label="{step.name}\\n({kind})"];')
        for src, dsts in graph.items():
            for dst in dsts:
                lines.append(f'  "{src}" -> "{dst}";')
        lines.append("}")
        click.echo("\n".join(lines))
    else:
        # ASCII: print level by level
        levels = execution_order(steps)
        for i, level in enumerate(levels):
            click.echo(f"Level {i}:")
            for s in level:
                inputs = ", ".join(s.input_names) if s.input_names else "—"
                downstream = ", ".join(graph.get(s.name, [])) or "—"
                click.echo(f"  {s.name}  [{s.kind.value}]  in=({inputs})  out=({downstream})")


# --------------------------------------------------------------------------- #
# conduit replay
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("step_name")
@click.option("--run", "run_id", default=None, help="Replay with inputs from a specific run ID")
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.pass_context
def replay(ctx: click.Context, step_name: str, run_id: str | None, pipeline: tuple[str, ...]) -> None:
    """Re-run a step locally using its last recorded inputs (for debugging)."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    from conduit_etl.core.registry import get_registry
    from conduit_etl.executor.local import LocalExecutor

    try:
        catalog = _make_catalog(cfg)
        registry = get_registry()

        try:
            step = registry.get(step_name)
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

        # Find the run to replay
        if run_id:
            run = catalog.get_run_by_id(run_id)
            if run is None:
                raise click.ClickException(f"run {run_id!r} not found")
            fp = run.fingerprint
        else:
            last = catalog.last_run(step_name, only_success=True)
            if last is None:
                raise click.ClickException(f"no successful run found for {step_name!r}")
            fp = last.fingerprint

        # Resolve inputs from fingerprint snapshot IDs — use the specific snapshot
        # from that run so replay is deterministic, not just "latest".
        inputs = {}
        for name in step.input_names:
            entry = fp.get(name)
            if entry and isinstance(entry, list):
                from conduit_etl.core.models import Snapshot
                from datetime import datetime as _dt
                snap_id = entry[0]
                stub = Snapshot(id=snap_id, table=name, created_at=_dt.now(), rows=0, schema_hash="")
                try:
                    inputs[name] = catalog.as_relation(stub)
                except Exception:
                    snap = catalog.latest_snapshot(name)
                    if snap:
                        inputs[name] = catalog.as_relation(snap)

        staging = cfg.executor.staging_path or cfg.steps.staging_path
        executor = LocalExecutor(workers=1, staging_path=staging)
        fut = executor.submit(step, inputs)
        result = fut.result()
        executor.shutdown(wait=False)
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    _emit({"step": step_name, "rows": result.rows, "duration_seconds": result.duration_seconds,
           "staging_path": result.staging_path}, output)


# --------------------------------------------------------------------------- #
# conduit backfill
# --------------------------------------------------------------------------- #

@main.command()
@click.argument("step_name")
@click.option("--date", "date_str", required=True, help="Partition date (YYYY-MM-DD)")
@click.option("--pipeline", "-p", multiple=True, help="Python module(s) containing your pipeline steps")
@click.pass_context
def backfill(ctx: click.Context, step_name: str, date_str: str, pipeline: tuple[str, ...]) -> None:
    """Re-run a step for a specific date partition and commit as a new snapshot."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    for mod in pipeline:
        try:
            importlib.import_module(mod)
        except ImportError as exc:
            raise click.ClickException(f"cannot import pipeline module {mod!r}: {exc}") from exc

    from conduit_etl.core.registry import get_registry
    from conduit_etl.executor.local import LocalExecutor
    from conduit_etl.core.fingerprint import compute_fingerprint

    try:
        catalog = _make_catalog(cfg)
        registry = get_registry()

        try:
            step = registry.get(step_name)
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc

        # Resolve full inputs (backfill always runs against current data, filtered by date)
        inputs = {}
        for name in step.input_names:
            snap = catalog.latest_snapshot(name)
            if snap:
                rel = catalog.as_relation(snap)
                if step.partition_by:
                    rel = _filter_partition(rel, step.partition_by, date_str)
                inputs[name] = rel

        staging = cfg.executor.staging_path or cfg.steps.staging_path
        executor = LocalExecutor(workers=1, staging_path=staging)
        fut = executor.submit(step, inputs)
        result = fut.result()
        executor.shutdown(wait=False)

        # Commit backfill result to catalog
        staged = catalog.staged_relation(result.staging_path)
        fp = compute_fingerprint(step, catalog)
        fp["__backfill_date__"] = date_str
        with catalog.transaction() as txn:
            meta = {"step": step_name, "merge": step.merge.value,
                    "merge_key": step.merge_key, "backfill_date": date_str}
            snap = txn.write(step.output_name, staged, meta)
            txn.commit()

        import uuid as _uuid
        from datetime import datetime as _dt
        from conduit_etl.core.models import RunRecord
        now = _dt.now()
        catalog.record_run(RunRecord(
            id=_uuid.uuid4().hex, step_name=step_name, output_table=step.output_name,
            status="success", snapshot_id=snap.id, fingerprint=fp,
            rows=result.rows, duration_seconds=result.duration_seconds,
            started_at=now, finished_at=now, error=None,
        ))
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    _emit({"step": step_name, "date": date_str, "rows": result.rows, "snapshot_id": snap.id}, output)


# --------------------------------------------------------------------------- #
# conduit catalog (subgroup)
# --------------------------------------------------------------------------- #

@main.group()
def catalog() -> None:
    """Catalog management commands."""


@catalog.command("snapshots")
@click.argument("table")
@click.pass_context
def catalog_snapshots(ctx: click.Context, table: str) -> None:
    """List snapshots for a table."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    try:
        cat = _make_catalog(cfg)
        from datetime import datetime as _dt, timedelta
        snaps = cat.snapshots_since(table, _dt(1970, 1, 1))
        cat.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    rows = [{"id": s.id, "table": s.table, "created_at": str(s.created_at),
             "rows": s.rows, "schema_hash": s.schema_hash} for s in snaps]
    _emit(rows, output)


@catalog.command("diff")
@click.argument("table")
@click.argument("snap1")
@click.argument("snap2")
@click.pass_context
def catalog_diff(ctx: click.Context, table: str, snap1: str, snap2: str) -> None:
    """Show row-level diff between two snapshots of a table."""
    cfg: PipelineConfig = ctx.obj["cfg"]

    try:
        cat = _make_catalog(cfg)
        from conduit_etl.core.models import Snapshot
        from datetime import datetime as _dt

        s1 = Snapshot(id=snap1, table=table, created_at=_dt.now(), rows=0, schema_hash="")
        s2 = Snapshot(id=snap2, table=table, created_at=_dt.now(), rows=0, schema_hash="")
        rel1 = cat.as_relation(s1)
        rel2 = cat.as_relation(s2)

        added = rel2.except_(rel1)
        removed = rel1.except_(rel2)

        click.echo(f"=== Added in {snap2} ===")
        for row in added.fetchall():
            click.echo(f"  + {row}")
        click.echo(f"=== Removed in {snap2} ===")
        for row in removed.fetchall():
            click.echo(f"  - {row}")
        cat.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc


@catalog.command("gc")
@click.option("--older-than", "older_than", default="30d", help="Delete snapshots older than this (e.g. 30d, 7d)")
@click.option("--dry-run", is_flag=True, default=False, help="Show what would be deleted without deleting")
@click.pass_context
def catalog_gc(ctx: click.Context, older_than: str, dry_run: bool) -> None:
    """Remove old snapshots from the catalog, keeping the latest per table."""
    cfg: PipelineConfig = ctx.obj["cfg"]
    output: str = ctx.obj["output"]

    from conduit_etl.core.models import parse_duration
    try:
        cutoff_delta = parse_duration(older_than)
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        cat = _make_catalog(cfg)
        from datetime import datetime as _dt
        cutoff = _dt.now() - cutoff_delta
        result = _do_gc(cat, cutoff, dry_run=dry_run)
        cat.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc

    _emit(result, output)


def _filter_partition(rel, column: str, value: str):
    """Filter a DuckDB relation on a partition column.

    Detects the column's type and casts the value appropriately so the filter
    works for VARCHAR, DATE, TIMESTAMP, INTEGER, and BIGINT columns.
    """
    col_types = dict(zip(rel.columns, [str(t) for t in rel.types]))
    col_type = col_types.get(column, "VARCHAR").upper()

    if col_type in ("DATE",):
        return rel.filter(f"CAST({column} AS DATE) = CAST('{value}' AS DATE)")
    if col_type in ("TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ"):
        return rel.filter(f"CAST({column} AS DATE) = CAST('{value}' AS DATE)")
    if col_type in ("INTEGER", "INT", "INT4", "INT2", "SMALLINT",
                    "BIGINT", "INT8", "HUGEINT"):
        # Partition value must be a valid integer
        try:
            int_val = int(value)
        except ValueError as exc:
            raise click.ClickException(
                f"partition_by column {column!r} is {col_type} "
                f"but date {value!r} is not a valid integer"
            ) from exc
        return rel.filter(f"{column} = {int_val}")
    # Default: VARCHAR / unknown — use string equality with a parameterised literal
    safe_value = value.replace("'", "''")
    return rel.filter(f"{column} = '{safe_value}'")


def _do_gc(catalog, cutoff: "datetime", *, dry_run: bool) -> dict:
    if dry_run:
        # For dry run: count what would be deleted via the run_log relation
        rel = catalog.run_log()
        cols = rel.columns
        rows = rel.fetchall()
        stale = []
        # Find the latest snapshot_id per table
        latest: dict[str, str] = {}
        for row in rows:
            data = dict(zip(cols, row))
            tbl = data.get("output_table", "")
            snap = data.get("snapshot_id")
            status = data.get("status", "")
            if status == "success" and snap and tbl not in latest:
                latest[tbl] = snap
        for row in rows:
            data = dict(zip(cols, row))
            finished = data.get("finished_at")
            status = data.get("status", "")
            snap = data.get("snapshot_id")
            tbl = data.get("output_table", "")
            if (
                status == "success"
                and snap
                and finished
                and finished < cutoff
                and latest.get(tbl) != snap
            ):
                stale.append({"id": data.get("id"), "step": data.get("step_name"), "snapshot_id": snap})
        return {"dry_run": True, "would_delete": len(stale), "records": stale}

    deleted = catalog.delete_old_runs(cutoff, keep_latest_per_table=True)
    return {"deleted": deleted, "cutoff": str(cutoff)}
