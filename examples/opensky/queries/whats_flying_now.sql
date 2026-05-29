-- What is currently in the air, grouped by country?
SELECT
    origin_country,
    count(*)              AS aircraft,
    avg(altitude_ft)      AS avg_alt_ft,
    avg(speed_kts)        AS avg_speed_kts,
    mode() WITHIN GROUP (ORDER BY flight_phase) AS dominant_phase
FROM lake.airspace_snapshot
GROUP BY origin_country
ORDER BY aircraft DESC
LIMIT 20;

-- Most airborne aircraft right now with full details
SELECT
    icao24, callsign, origin_country,
    altitude_ft, speed_kts, heading_cardinal, flight_phase,
    nearest_airport_name, nearest_airport_km
FROM lake.airspace_snapshot
ORDER BY altitude_ft DESC
LIMIT 30;
