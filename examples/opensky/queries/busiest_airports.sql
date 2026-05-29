-- Busiest airports right now (approach + departure in last 30 min)
SELECT
    nearest_airport_name,
    nearest_airport_country,
    sum(aircraft_count)   AS total_aircraft,
    sum(CASE WHEN flight_phase = 'approach'   THEN aircraft_count ELSE 0 END) AS approaching,
    sum(CASE WHEN flight_phase = 'departure'  THEN aircraft_count ELSE 0 END) AS departing
FROM lake.airport_traffic
WHERE window_start > now() - INTERVAL '30 minutes'
GROUP BY nearest_airport_name, nearest_airport_country
ORDER BY total_aircraft DESC
LIMIT 20;

-- Approach traffic at a specific airport over the last hour
SELECT
    window_start,
    flight_phase,
    aircraft_count,
    min_altitude_ft
FROM lake.airport_traffic
WHERE nearest_airport_icao = 'EGLL'
  AND window_start > now() - INTERVAL '1 hour'
ORDER BY window_start DESC;
