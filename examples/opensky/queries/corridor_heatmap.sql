-- Top 20 busiest grid squares in the last 5 minutes
SELECT
    grid_lat, grid_lon,
    aircraft_count,
    avg_altitude_ft,
    dominant_phase
FROM lake.busy_corridors
WHERE window_start > now() - INTERVAL '5 minutes'
ORDER BY aircraft_count DESC
LIMIT 20;

-- Flight level usage — which altitudes are most popular?
SELECT
    floor(altitude_ft / 1000) * 1000  AS flight_level_ft,
    count(*)                           AS aircraft
FROM lake.flight_states_clean
WHERE snapshot_at > now() - INTERVAL '5 minutes'
GROUP BY flight_level_ft
ORDER BY flight_level_ft;

-- Aircraft airborne for more than 2 hours
SELECT
    icao24, callsign, origin_country,
    min(last_contact_at)  AS first_seen,
    max(last_contact_at)  AS last_seen,
    datediff('minute', min(last_contact_at), max(last_contact_at)) AS minutes_airborne
FROM lake.flight_states_clean
GROUP BY icao24, callsign, origin_country
HAVING minutes_airborne > 120
ORDER BY minutes_airborne DESC;
