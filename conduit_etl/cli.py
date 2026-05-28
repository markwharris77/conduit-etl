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
    from conduit_etl.catalog.local import LocalCatalog
    return LocalCatalog(cfg.catalog.path)


def _make_queue(cfg: PipelineConfig):
    from conduit_etl.queue.memory import MemoryQueue
    return MemoryQueue()


def _make_executor(cfg: PipelineConfig):
    from conduit_etl.executor.local import LocalExecutor
    staging = cfg.executor.staging_path or cfg.steps.staging_path
    return LocalExecutor(workers=cfg.executor.workers, staging_path=staging)


def _make_runtime(cfg: PipelineConfig, *, tags=None, steps=None, tick_interval=None):
    from conduit_etl.core.registry import get_registry
    from conduit_etl.core.runtime import Runtime
    from conduit_etl.core.models import parse_duration

    catalog = _make_catalog(cfg)
    queue = _make_queue(cfg)
    executor = _make_executor(cfg)
    registry = get_registry()

    interval = tick_interval
    if interval is None:
        interval = parse_duration(cfg.scheduler.tick).total_seconds()

    return Runtime(
        catalog=catalog,
        queue=queue,
        executor=executor,
        registry=registry,
        tick_interval=interval,
        tags=tags,
        step_names=steps,
    ), catalog


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
        runtime, catalog = _make_runtime(cfg, tags=tags, steps=step_names)
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
@click.pass_context
def scheduler(ctx: click.Context, pipeline: tuple[str, ...], tick: str | None) -> None:
    """Start the continuous scheduler daemon."""
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
        runtime, catalog = _make_runtime(cfg, tick_interval=tick_interval)
        runtime.run_forever()
        catalog.close()
    except ConduitError as exc:
        raise click.ClickException(str(exc)) from exc


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
            click.echo("No steps registered.")
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

    con = catalog.connection()
    if at:
        click.echo(f"Time-travel to {at!r} is not yet supported in Phase 1 — showing current state.")

    click.echo("conduit debug — DuckDB shell. Catalog attached as 'lake', run log as 'runs'.")
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
