# conduit-etl example — OpenSky live flight pipeline

A working pipeline that ingests live aircraft position data from the
[OpenSky Network](https://opensky-network.org), joins it against airport
reference data, and produces a set of continuously updated analytical tables.

No API key required. No infrastructure beyond conduit-etl itself.

---

## What it does

```
OpenSky API (every 30s)       OurAirports CSV (every 24h)
        │                               │
  flight_states_raw             airports_ref
        │                               │
        └──────────┬────────────────────┘
                   │
          flight_states_clean       ← filter + unit conversions
                   │
          flight_states_enriched    ← nearest airport + flight phase
                   │
        ┌──────────┼──────────┬──────────┐
        │          │          │          │
  airspace_snapshot  country_stats  airport_traffic  busy_corridors
  (live view,      (5-min counts   (per-airport     (0.5° grid
   upsert on        by country)     traffic)         heatmap)
   icao24)
```

After one tick you have six tables in the catalog, all time-travel queryable.
`flight_states_clean` and `flight_states_enriched` accumulate history on every
tick — the skip logic means downstream steps only re-run when new aircraft
positions arrive.

---

## Requirements

- conduit-etl installed (from the parent directory)
- Python 3.12+
- Internet access (OpenSky and OurAirports are public APIs)

---

## Quickstart

```bash
cd examples/opensky
export PYTHONPATH=.

# Run once — fetches live data and exits
conduit --config pipeline.toml run \
  --pipeline pipeline.sources.opensky \
  --pipeline pipeline.sources.airports \
  --pipeline pipeline.steps.clean \
  --pipeline pipeline.steps.enrich \
  --pipeline pipeline.steps.snapshot \
  --pipeline pipeline.steps.country_stats \
  --pipeline pipeline.steps.airport_traffic \
  --pipeline pipeline.steps.corridors \
  --pipeline pipeline.sinks.parquet
```

Typical first run takes about 15 seconds (airports CSV download dominates).
Subsequent runs are ~5 seconds each — airports is skipped until 24h have passed.

Check what ran:

```bash
conduit --config pipeline.toml history
```

---

## Continuous mode

```bash
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
```

The scheduler ticks every 30 seconds. `flight_states_clean` and
`flight_states_enriched` use `incremental=True, merge=append`, so each tick
appends new positions to the catalog. After a few minutes you have enough
history to query time-series patterns.

---

## Querying the data

### Option A — DuckDB REPL via conduit debug

```bash
conduit --config pipeline.toml debug
```

```sql
-- What is in the air right now?
SELECT origin_country, count(*) AS aircraft, avg(altitude_ft) AS avg_alt
FROM lake.airspace_snapshot
GROUP BY origin_country ORDER BY aircraft DESC LIMIT 10;

-- Busiest airports by total traffic
SELECT nearest_airport_name, nearest_airport_country,
       sum(aircraft_count) AS total
FROM lake.airport_traffic
GROUP BY nearest_airport_name, nearest_airport_country
ORDER BY total DESC LIMIT 10;

-- Flight phase breakdown
SELECT flight_phase, count(*) AS aircraft
FROM lake.airspace_snapshot
GROUP BY flight_phase ORDER BY aircraft DESC;

-- Hottest grid squares right now
SELECT grid_lat, grid_lon, aircraft_count, dominant_phase
FROM lake.busy_corridors
ORDER BY aircraft_count DESC LIMIT 20;

-- Aircraft that have been airborne longest (needs a few ticks of history)
SELECT icao24, callsign, origin_country,
       min(last_contact_at) AS first_seen,
       max(last_contact_at) AS last_seen,
       datediff('minute', min(last_contact_at), max(last_contact_at)) AS minutes_airborne
FROM lake.flight_states_clean
GROUP BY icao24, callsign, origin_country
HAVING minutes_airborne > 60
ORDER BY minutes_airborne DESC;
```

Type `.quit` or Ctrl-D to exit.

### Option B — query the parquet output files directly

The sink writes to `output/` after every tick. Query without opening the catalog:

```bash
duckdb -c "
SELECT origin_country, count(*) AS aircraft
FROM 'output/airspace_snapshot/latest.parquet'
GROUP BY origin_country ORDER BY aircraft DESC LIMIT 10
"
```

### Option C — time-travel in the REPL

Every catalog write is a DuckLake snapshot. Use `AT (VERSION => N)` to query
any point in history:

```sql
-- List all snapshots for a table
SELECT * FROM ducklake_snapshots('lake') ORDER BY snapshot_id DESC;

-- What did the airspace look like at snapshot 5?
SELECT origin_country, count(*) AS aircraft
FROM lake.airspace_snapshot AT (VERSION => 5)
GROUP BY origin_country ORDER BY aircraft DESC;
```

---

## Pipeline details

| Step | Schedule | Output rows (typical) | Notes |
|------|----------|-----------------------|-------|
| `flight_states_raw` | 30s | ~1500 | Europe bounding box |
| `airports_ref` | 24h | ~5000 | large + medium airports |
| `flight_states_clean` | on change | ~1400 | airborne only, derived columns |
| `flight_states_enriched` | on change | ~1400 | nearest airport + flight phase |
| `airspace_snapshot` | on change | ~1400 | one row per live aircraft |
| `country_stats` | on change | ~150 | 5-min windows |
| `airport_traffic` | on change | ~800 | within 100km of airport |
| `busy_corridors` | on change | ~1000 | 0.5° grid squares |

### Flight phase logic

| Phase | Condition |
|-------|-----------|
| cruise | altitude > 30,000 ft |
| climb | vertical rate > 500 ft/min |
| descent | vertical rate < −500 ft/min |
| approach | altitude < 5,000 ft, within 50 km of airport, descending |
| departure | altitude < 5,000 ft, within 50 km of airport, climbing |
| level | everything else |

---

## Project structure

```
examples/opensky/
├── pipeline.toml               ← conduit config (local catalog, memory queue)
├── pipeline/
│   ├── sources/
│   │   ├── opensky.py          ← flight_states_raw
│   │   └── airports.py         ← airports_ref
│   ├── steps/
│   │   ├── clean.py            ← flight_states_clean
│   │   ├── enrich.py           ← flight_states_enriched
│   │   ├── snapshot.py         ← airspace_snapshot
│   │   ├── country_stats.py    ← country_stats
│   │   ├── airport_traffic.py  ← airport_traffic
│   │   └── corridors.py        ← busy_corridors
│   └── sinks/
│       └── parquet.py          ← writes output/ parquet files
├── queries/
│   ├── whats_flying_now.sql
│   ├── busiest_airports.sql
│   └── corridor_heatmap.sql
└── tests/
    ├── fixtures/
    │   ├── opensky_response.json   ← 10-aircraft sample (no live API in tests)
    │   └── airports_sample.csv     ← 50 major European airports
    ├── test_clean.py
    ├── test_enrich.py
    └── test_aggregations.py
```

---

## Running the tests

```bash
cd examples/opensky
pytest tests/
```

All 21 tests run against fixture data — no network calls required.

---

## Data sources

- **OpenSky Network** — `https://opensky-network.org/api/states/all`
  Anonymous tier, no key required. Rate limited to 10 req/10s.
  Returns current state vectors (~1500 aircraft over Europe).

- **OurAirports** — `https://ourairports.com/data/airports.csv`
  Free, updated daily, no authentication. ~74,000 airports worldwide;
  this pipeline filters to large and medium airports (~5000).
