#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"
export PYTHONPATH=.

conduit --config pipeline.toml scheduler \
  --pipeline pipeline.sources.opensky \
  --pipeline pipeline.sources.airports \
  --pipeline pipeline.steps.clean \
  --pipeline pipeline.steps.enrich \
  --pipeline pipeline.steps.snapshot \
  --pipeline pipeline.steps.country_stats \
  --pipeline pipeline.steps.airport_traffic \
  --pipeline pipeline.steps.corridors \
  --pipeline pipeline.sinks.parquet
