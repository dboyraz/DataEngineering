import os
from pyspark.sql import SparkSession

os.environ["HADOOP_HOME"] = r"C:\Users\deniz\Desktop\Batch\hadoop"
os.environ["PATH"] = r"C:\Users\deniz\Desktop\Batch\hadoop\bin" + os.pathsep + os.environ["PATH"]


def main():
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("YellowTaxiPipeline") \
        .getOrCreate()

    # Read the parquet file into a DataFrame
    df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
    print(f"Row count: {df.count()}")
    df.printSchema()

    # Repartition to 4 partitions and save
    output_path = "output/yellow_tripdata_2025-11_repartitioned"
    df.repartition(4).write.mode("overwrite").parquet(output_path)
    print(f"Saved to {output_path}")

    # Calculate average size of the output .parquet files in MB
    parquet_files = [
        f for f in os.listdir(output_path) if f.endswith(".parquet")
    ]
    sizes_mb = [
        os.path.getsize(os.path.join(output_path, f)) / (1024 * 1024)
        for f in parquet_files
    ]
    print(f"Number of parquet files: {len(parquet_files)}")
    print(f"File sizes (MB): {[round(s, 2) for s in sizes_mb]}")
    print(f"Average parquet file size: {round(sum(sizes_mb) / len(sizes_mb), 2)} MB")

    # Count trips that started on November 15th
    trips_nov15 = df.filter(
        (df.tpep_pickup_datetime >= "2025-11-15 00:00:00") &
        (df.tpep_pickup_datetime < "2025-11-16 00:00:00")
    ).count()
    print(f"Trips starting on November 15th: {trips_nov15}")

    # Longest trip duration in hours
    from pyspark.sql.functions import col, max as spark_max, unix_timestamp
    longest_hours = df.select(
        spark_max(
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600
        ).alias("longest_trip_hours")
    ).collect()[0]["longest_trip_hours"]
    print(f"Longest trip duration: {round(longest_hours, 2)} hours")

    # Load zone lookup CSV into a temp view
    zones = spark.read.option("header", True).csv("taxi_zone_lookup.csv")
    zones.createOrReplaceTempView("zones")

    # Find the least frequent pickup location zone
    df.createOrReplaceTempView("trips")
    result = spark.sql("""
        SELECT z.Zone, COUNT(*) AS trip_count
        FROM trips t
        JOIN zones z ON t.PULocationID = z.LocationID
        GROUP BY z.Zone
        ORDER BY trip_count ASC
        LIMIT 1
    """)
    result.show(truncate=False)


if __name__ == "__main__":
    main()
