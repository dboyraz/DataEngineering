# SQL Queries Used

## 1) Count of records for 2024 Yellow Taxi data (local parquet)

```sql
SELECT COUNT(*) AS records_2024
FROM read_parquet('data/yellow_tripdata_2024-*.parquet');
```

## 2) Distinct `PULocationID` count (external table)

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM `your_project.your_dataset.yellow_taxi_external`;
```

## 3) Distinct `PULocationID` count (materialized/native table)

```sql
SELECT COUNT(DISTINCT PULocationID) AS distinct_pu_location_ids
FROM `your_project.your_dataset.yellow_taxi`;
```

## 4) Retrieve `PULocationID` only (materialized/native table)

```sql
SELECT PULocationID
FROM `your_project.your_dataset.yellow_taxi`;
```

## 5) Retrieve `PULocationID` and `DOLocationID` (materialized/native table)

```sql
SELECT PULocationID, DOLocationID
FROM `your_project.your_dataset.yellow_taxi`;
```

## 6) Count records with `fare_amount = 0` (local parquet)

```sql
SELECT COUNT(*) AS records_with_zero_fare
FROM read_parquet('data/yellow_tripdata_2024-*.parquet')
WHERE fare_amount = 0;
```

## 7) Create optimized partitioned + clustered table in BigQuery

```sql
CREATE OR REPLACE TABLE `your_project.your_dataset.yellow_taxi_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT *
FROM `your_project.your_dataset.yellow_taxi`;
```

## 8) Distinct `VendorID` for 2024-03-01 to 2024-03-15 (materialized/non-partitioned table)

```sql
SELECT DISTINCT VendorID
FROM `your_project.your_dataset.yellow_taxi`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime < '2024-03-16';
```

## 9) Distinct `VendorID` for 2024-03-01 to 2024-03-15 (optimized partitioned table)

```sql
SELECT DISTINCT VendorID
FROM `your_project.your_dataset.yellow_taxi_optimized`
WHERE tpep_dropoff_datetime >= '2024-03-01'
  AND tpep_dropoff_datetime < '2024-03-16';
```

## 10) `COUNT(*)` from the materialized/native table

```sql
SELECT COUNT(*)
FROM `your_project.your_dataset.yellow_taxi`;
```
