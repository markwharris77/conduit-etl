# conduit-etl example — OpenSky live flight pipeline

A real-world example pipeline for conduit-etl that ingests live aircraft
position data from the OpenSky Network, enriches it with airport reference
data, and produces a set of continuously updated analytical tables.

Demonstrates: @source polling, daily reference joins, incremental @step,
skip logic, time-travel debugging, and a meaningful cross-source join —
all with zero infrastructure beyond conduit-etl itself.

---

## What the pipeline does

```
OpenSky API (every 30s)          OurAirports CSV (daily)
        │                                  │
   @source                            @source
        │                                  │
  flight_states_raw              airports_ref
        │                                  │
        └──────────┬───────────────────────┘
                   │
            @step: clean
                   │
          flight_states_clean
                   │
            @step: enrich (cross-source join to airports_ref)
                   │
         flight_states_enriched
                   │
        ┌──────────┼──────────┬──────────┐
        │          │          │          │
   @step        @step      @step      @step
  airspace    country    airport    corridors
  snapshot     stats     traffic
        │          │          │          │
   one row    counts    approach    grid
   per live   by flag   /depart     density
   aircraft   + phase   detection   map
```

Six tables in the catalog by the end of one tick. All time-travel queryable.

---

## API documentation

### OpenSky Network

**Base URL:** `https://opensky-network.org/api`

No API key required for the anonymous tier. Anonymous requests are rate
limited to 10 requests per 10 seconds and return current state only
(no historical data). Authenticated requests unlock 30-day history.

Register free at: https://opensky-network.org/index.php?option=com_users&view=registration

#### GET /states/all

Returns state vectors for all aircraft currently tracked by OpenSky.

**Query parameters:**

| Parameter | Type   | Description |
|-----------|--------|-------------|
| time      | int    | Unix timestamp. Returns state at that time (authenticated only). Omit for current. |
| icao24    | string | Filter by ICAO24 transponder address (hex). Comma-separated for multiple. |
| lamin     | float  | Bounding box: minimum latitude  |
| lomin     | float  | Bounding box: minimum longitude |
| lamax     | float  | Bounding box: maximum latitude  |
| lomax     | float  | Bounding box: maximum longitude |

**Example — Europe bounding box:**
```
GET https://opensky-network.org/api/states/all?lamin=35.0&lomin=-15.0&lamax=72.0&lomax=42.0
```

**Response:**
```json
{
  "time": 1716825600,
  "states": [
    [
      "3c6444",       // 0  icao24          — ICAO 24-bit transponder address (hex)
      "DLH123  ",     // 1  callsign        — may have trailing spaces
      "Germany",      // 2  origin_country
      1716825598,     // 3  time_position   — unix, when position was last updated
      1716825599,     // 4  last_contact    — unix, when transponder last contacted
      8.5622,         // 5  longitude       — WGS84 decimal degrees
      50.0379,        // 6  latitude        — WGS84 decimal degrees
      10972.8,        // 7  baro_altitude   — metres, barometric. null if on ground
      false,          // 8  on_ground       — true if squawking on-ground
      245.3,          // 9  velocity        — m/s ground speed
      270.0,          // 10 true_track      — degrees clockwise from north
      0.0,            // 11 vertical_rate   — m/s, positive = climbing
      null,           // 12 sensors         — serial numbers of receivers, often null
      11277.6,        // 13 geo_altitude    — metres, GPS altitude
      "1000",         // 14 squawk          — transponder code
      false,          // 15 spi             — special purpose indicator
      0               // 16 position_source — 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
    ]
  ]
}
```

**Notes:**
- `states` is null if no aircraft in the requested area
- `baro_altitude` and `geo_altitude` are null for on-ground aircraft
- `callsign` has trailing spaces — always trim
- Anonymous tier: current state only, ~6,400 aircraft worldwide, ~1,500 over Europe
- Bounding box is recommended — without it the response is ~2MB

#### GET /flights/aircraft

Returns flights for a specific aircraft over a time range (authenticated only).

```
GET /flights/aircraft?icao24=3c6444&begin=1716739200&end=1716825600
```

Not used in this pipeline but useful for debugging specific aircraft.

---

### OurAirports reference data

**URL:** `https://ourairports.com/data/airports.csv`

Free, updated daily, no authentication. ~74,000 airports worldwide.
Download once daily via a `@source(schedule="daily")` — no need to hit it more.

**Relevant columns:**

| Column        | Type   | Description |
|---------------|--------|-------------|
| ident         | string | ICAO code (e.g. EGLL for Heathrow). Primary join key. |
| name          | string | Full airport name |
| type          | string | large_airport, medium_airport, small_airport, heliport, seaplane_base, closed |
| latitude_deg  | float  | Decimal degrees |
| longitude_deg | float  | Decimal degrees |
| elevation_ft  | int    | Elevation above sea level |
| iso_country   | string | ISO 3166-1 alpha-2 country code |
| iso_region    | string | ISO 3166-2 region code |
| municipality  | string | Nearest city |
| iata_code     | string | IATA code if assigned (e.g. LHR). May be empty. |

**Filter to:** `type IN ('large_airport', 'medium_airport')` — reduces to ~7,000
airports, which covers all commercial aviation and most GA fields worth naming.

---

## Pipeline design

### Sources

#### `flight_states_raw`
- **Schedule:** every 30 seconds
- **API:** OpenSky `/states/all` with Europe bounding box
  `lamin=35.0, lomin=-15.0, lamax=72.0, lomax=42.0`
- **Watermark:** `last_contact` (unix timestamp)
  Only rows with `last_contact` newer than the watermark from the last run
  are emitted downstream. If OpenSky returns no updated positions, this
  source returns empty and all downstream steps are skipped automatically.
- **Output columns:**
  icao24, callsign, origin_country, time_position, last_contact,
  longitude, latitude, baro_altitude, on_ground, velocity, true_track,
  vertical_rate, geo_altitude, squawk, position_source, server_time

#### `airports_ref`
- **Schedule:** daily (at midnight)
- **API:** OurAirports CSV download
- **Filter:** large_airport and medium_airport only
- **Output columns:**
  icao_code, airport_name, country_code, region_code, municipality,
  airport_type, latitude, longitude, elevation_ft, iata_code

---

### Steps

#### `flight_states_clean`
- **Inputs:** `flight_states_raw`
- **Incremental:** yes, append
- **Purpose:** cast, filter, derive useful columns
- **Filters applied:**
  - `on_ground = false` — airborne only
  - `longitude IS NOT NULL AND latitude IS NOT NULL`
  - `baro_altitude > 0`
  - `velocity > 0`
  - `icao24 NOT IN ('000000', '')` — bad transponders
- **Derived columns:**

| Column          | Derivation |
|-----------------|------------|
| altitude_ft     | baro_altitude × 3.28084 |
| speed_kts       | velocity × 1.94384 (m/s → knots) |
| vertical_fpm    | vertical_rate × 196.85 (m/s → ft/min) |
| heading_deg     | true_track cast to int |
| heading_cardinal | N/E/S/W bucketed from heading_deg |
| last_contact_at | to_timestamp(last_contact) |
| snapshot_at     | to_timestamp(server_time) |
| position_source_name | CASE 0→'ADS-B' 1→'ASTERIX' 2→'MLAT' 3→'FLARM' |

#### `flight_states_enriched`
- **Inputs:** `flight_states_clean`, `airports_ref`
- **Incremental:** yes, append
- **Purpose:** join each aircraft to nearest large/medium airport using
  equirectangular distance approximation. No PostGIS required.
- **Join logic:**
  Pre-filter airports to a 3°×4° bounding box around each aircraft,
  then compute exact distance, rank, take nearest within 370km (200nm).
- **Additional derived columns:**

| Column                  | Description |
|-------------------------|-------------|
| nearest_airport_icao    | ICAO code of nearest airport |
| nearest_airport_name    | Full name |
| nearest_airport_country | ISO country code |
| nearest_airport_region  | ISO region code |
| nearest_airport_km      | Distance in km, rounded to 1dp |
| flight_phase            | cruise / climb / descent / approach / departure / level |

**Flight phase logic:**

```
altitude_ft > 30000                              → cruise
vertical_fpm > 500                               → climb
vertical_fpm < -500                              → descent
altitude_ft < 5000 AND nearest_airport_km < 50
  AND vertical_fpm < -200                        → approach
altitude_ft < 5000 AND nearest_airport_km < 50
  AND vertical_fpm > 200                         → departure
else                                             → level
```

#### `airspace_snapshot`
- **Inputs:** `flight_states_enriched`
- **Incremental:** yes, **upsert on icao24**
- **Purpose:** current live view — one row per aircraft, always up to date.
  This is the table you'd query for "what is in the air right now."
- **Output:** same columns as `flight_states_enriched`, latest row per icao24

#### `country_stats`
- **Inputs:** `flight_states_enriched`
- **Incremental:** yes, **upsert on (origin_country, window_start)**
- **Purpose:** rolling 5-minute counts by origin country and flight phase.
  Answers "how many German aircraft are currently in cruise?"
- **Output columns:**

| Column         | Description |
|----------------|-------------|
| origin_country | Aircraft registration country |
| window_start   | 5-minute bucket (date_trunc('5 minutes', snapshot_at)) |
| flight_phase   | cruise / climb / descent / approach / departure / level |
| aircraft_count | Distinct icao24 in this window+country+phase |
| avg_altitude_ft | Mean altitude |
| avg_speed_kts  | Mean ground speed |

#### `airport_traffic`
- **Inputs:** `flight_states_enriched`
- **Incremental:** yes, **upsert on (nearest_airport_icao, window_start)**
- **Purpose:** per-airport traffic counts in 5-minute windows.
  Answers "how many aircraft are currently in the approach phase at Heathrow?"
- **Filter:** `nearest_airport_km < 100` — only count aircraft within 100km
- **Output columns:**

| Column               | Description |
|----------------------|-------------|
| nearest_airport_icao | ICAO of the airport |
| nearest_airport_name | Airport name |
| nearest_airport_country | Country |
| window_start         | 5-minute bucket |
| flight_phase         | approach / departure / cruise / etc. |
| aircraft_count       | Distinct aircraft in window |
| min_altitude_ft      | Lowest aircraft (useful for approach detection) |

#### `busy_corridors`
- **Inputs:** `flight_states_enriched`
- **Incremental:** yes, **upsert on (grid_lat, grid_lon, window_start)**
- **Purpose:** density heatmap — how many aircraft passed through each
  0.5°×0.5° grid square in the last 5 minutes. Reveals airways and
  busy corridors without needing actual route data.
- **Grid derivation:**
  `grid_lat = floor(latitude  / 0.5) * 0.5`
  `grid_lon = floor(longitude / 0.5) * 0.5`
- **Output columns:**

| Column         | Description |
|----------------|-------------|
| grid_lat       | SW corner latitude of grid square |
| grid_lon       | SW corner longitude of grid square |
| window_start   | 5-minute bucket |
| aircraft_count | Distinct aircraft in grid square in window |
| avg_altitude_ft | Mean altitude (reveals flight levels in use) |
| avg_speed_kts  | Mean speed |
| dominant_phase | Most common flight phase in this grid square |

---

### Sinks

#### `parquet_sink`
- **Input:** all six output tables
- **Output path:** `./output/{table_name}/` — one Parquet file per run
- **Purpose:** historical archive, queryable by DuckDB after the fact
- **Use:** `conduit debug` then `SELECT * FROM 'output/country_stats/*.parquet'`

---

## Project structure

```
conduit-opensky/
├── CLAUDE.md                    ← this file
├── pipeline.toml                ← conduit config
├── pipeline/
│   ├── __init__.py
│   ├── sources/
│   │   ├── __init__.py
│   │   ├── opensky.py           ← flight_states_raw
│   │   └── airports.py          ← airports_ref
│   ├── steps/
│   │   ├── __init__.py
│   │   ├── clean.py             ← flight_states_clean
│   │   ├── enrich.py            ← flight_states_enriched
│   │   ├── snapshot.py          ← airspace_snapshot
│   │   ├── country_stats.py     ← country_stats
│   │   ├── airport_traffic.py   ← airport_traffic
│   │   └── corridors.py         ← busy_corridors
│   └── sinks/
│       ├── __init__.py
│       └── parquet.py           ← parquet_sink
├── tests/
│   ├── fixtures/
│   │   ├── opensky_response.json   ← sample API response for tests
│   │   └── airports_sample.csv     ← 50-row airports subset for tests
│   ├── test_clean.py
│   ├── test_enrich.py
│   └── test_aggregations.py
└── queries/
    ├── whats_flying_now.sql        ← example queries for conduit debug
    ├── busiest_airports.sql
    └── corridor_heatmap.sql
```

---

## Example queries (for `conduit debug`)

After the pipeline has run, drop into `conduit debug` and try:

```sql
-- What's in the air right now?
SELECT origin_country, count(*) AS aircraft, avg(altitude_ft) AS avg_alt_ft
FROM airspace_snapshot
GROUP BY origin_country
ORDER BY aircraft DESC
LIMIT 10;

-- Busiest approach corridors in the last hour
SELECT nearest_airport_name, nearest_airport_country,
       sum(aircraft_count) AS total_aircraft
FROM airport_traffic
WHERE flight_phase = 'approach'
  AND window_start > now() - interval '1 hour'
GROUP BY nearest_airport_name, nearest_airport_country
ORDER BY total_aircraft DESC
LIMIT 10;

-- Flight level distribution — which altitudes are most used?
SELECT
  floor(altitude_ft / 1000) * 1000 AS flight_level_ft,
  count(*) AS aircraft
FROM flight_states_clean
WHERE snapshot_at > now() - interval '5 minutes'
GROUP BY flight_level_ft
ORDER BY flight_level_ft;

-- Time-travel: what was the busiest corridor yesterday at 08:00?
-- (conduit debug --at "yesterday 08:00" then:)
SELECT grid_lat, grid_lon, aircraft_count
FROM busy_corridors
ORDER BY aircraft_count DESC
LIMIT 20;

-- Aircraft that have been airborne the longest (continuous icao24 presence)
SELECT icao24, callsign, origin_country,
       min(last_contact_at) AS first_seen,
       max(last_contact_at) AS last_seen,
       datediff('minute', min(last_contact_at), max(last_contact_at)) AS minutes_airborne
FROM flight_states_clean
GROUP BY icao24, callsign, origin_country
HAVING minutes_airborne > 120
ORDER BY minutes_airborne DESC;
```

---

## Running the pipeline

```bash
# Install conduit-etl (assumes built from phase 1)
cd ../conduit-etl
uv pip install -e .

# Move to example
cd ../conduit-opensky
uv venv
uv pip install -e "../conduit-etl"

# Run once (good for testing)
conduit run

# Run continuously (normal operating mode)
conduit scheduler

# Inspect what ran
conduit history

# Drop into DuckDB with the catalog
conduit debug

# See what's in the air right now from the REPL
-- SELECT * FROM airspace_snapshot ORDER BY altitude_ft DESC LIMIT 20;
```

---

## Test fixtures

The tests should not hit the live API. Include these fixtures:

### `tests/fixtures/opensky_response.json`

A real-looking sample response with 10 aircraft over Europe.
Include a mix of: cruising at FL350, one on approach, one climbing,
one with null position (to test filtering), one with icao24='000000'
(to test bad transponder filtering).

### `tests/fixtures/airports_sample.csv`

50 rows from OurAirports covering major European hubs:
EGLL (Heathrow), EDDF (Frankfurt), LFPG (CDG), EHAM (Schiphol),
LEMD (Madrid), LIRF (Rome), LSZH (Zurich), LOWW (Vienna).
Include at least one small_airport to test the type filter.

---

## Handoff notes for Claude Code

Build in this order:

1. `tests/fixtures/` — create both fixture files first
2. `pipeline/sources/opensky.py` — source with mock-friendly HTTP call
3. `pipeline/sources/airports.py` — CSV download source
4. `pipeline/steps/clean.py` — pure SQL, test against fixture
5. `pipeline/steps/enrich.py` — the cross-source join
6. `pipeline/steps/snapshot.py` — upsert step
7. `pipeline/steps/country_stats.py`
8. `pipeline/steps/airport_traffic.py`
9. `pipeline/steps/corridors.py`
10. `pipeline/sinks/parquet.py`
11. `queries/` — example SQL files
12. Integration test: run the full pipeline against fixtures end to end

Each step should be tested with `step.__wrapped__()` and an in-memory
DuckDB relation built from the fixture data. No live API calls in tests.

The enrich step (cross-source join) is the most complex — test it with
an aircraft known to be near Heathrow and assert the correct airport
is returned, and an aircraft over the mid-Atlantic and assert no airport
match (null nearest_airport_icao).