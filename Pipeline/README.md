## Docker

Run a container with `python:3.13` and `bash` as the entrypoint:

```powershell
docker run -it --rm --entrypoint bash python:3.13
```

## SQL Queries

Trips in November 2025 with trip distance <= 1 mile:

```sql
SELECT COUNT(*) AS trips_le_1mi
FROM green_tripdata_2025_11
WHERE lpep_pickup_datetime >= '2025-11-01'
  AND lpep_pickup_datetime <  '2025-12-01'
  AND trip_distance <= 1;
```

Pickup day with the longest trip distance (excluding >= 100 miles):

```sql
SELECT
  DATE(lpep_pickup_datetime) AS pickup_day,
  MAX(trip_distance) AS max_trip_distance
FROM green_tripdata_2025_11
WHERE trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_trip_distance DESC
LIMIT 1;
```

Pickup zone with largest total amount on 2025-11-18:

```sql
SELECT
  z."Zone" AS pickup_zone,
  SUM(t.total_amount) AS total_amount_sum
FROM green_tripdata_2025_11 t
JOIN taxi_zone_lookup z
  ON t."PULocationID" = z."LocationID"
WHERE t.lpep_pickup_datetime >= '2025-11-18'
  AND t.lpep_pickup_datetime <  '2025-11-19'
GROUP BY z."Zone"
ORDER BY total_amount_sum DESC
LIMIT 1;
```

Dropoff zone with the largest tip for pickups in East Harlem North:

```sql
SELECT
  dz."Zone" AS dropoff_zone,
  MAX(t.tip_amount) AS max_tip
FROM green_tripdata_2025_11 t
JOIN taxi_zone_lookup pz ON t."PULocationID" = pz."LocationID"
JOIN taxi_zone_lookup dz ON t."DOLocationID" = dz."LocationID"
WHERE pz."Zone" = 'East Harlem North'
  AND t.lpep_pickup_datetime >= '2025-11-01'
  AND t.lpep_pickup_datetime <  '2025-12-01'
GROUP BY dz."Zone"
ORDER BY max_tip DESC
LIMIT 1;
```
