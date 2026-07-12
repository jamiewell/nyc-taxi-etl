"""
Cleaned to Fact & Dimension Layer
Transform cleaned data to fact_taxi_trip and build dimension tables
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, monotonically_increasing_id, current_timestamp,
    round as spark_round, when, lit
)


def read_cleaned_data(spark, cleaned_path):
    """
    Read cleaned layer data
    """
    print(f"Reading cleaned data from: {cleaned_path}")

    try:
        df = spark.read.parquet(cleaned_path)
        record_count = df.count()
        print(f"Cleaned records loaded: {record_count:,}")
        return df
    except Exception as e:
        raise ValueError(f"Failed to read cleaned data from {cleaned_path}. Error: {str(e)}")


def build_dim_vendor(df_cleaned: DataFrame) -> DataFrame:
    """
    Build dim_vendor dimension table from cleaned data

    Grain: 1 row = 1 vendor
    Columns: vendor_id, vendor_name
    """
    print("\n--- Building dim_vendor ---")

    # Extract unique vendors and map to vendor names
    dim_vendor = df_cleaned.select("vendor_id").distinct() \
        .withColumn("vendor_name",
                   when(col("vendor_id") == 1, lit("Creative Mobile Technologies"))
                   .when(col("vendor_id") == 2, lit("VeriFone"))
                   .otherwise(lit("Unknown"))) \
        .orderBy("vendor_id")

    vendor_count = dim_vendor.count()
    print(f"dim_vendor records: {vendor_count}")
    dim_vendor.show(10, truncate=False)

    return dim_vendor


def build_dim_taxi_zone(spark, zone_lookup_path: str) -> DataFrame:
    """
    Build dim_taxi_zone dimension table from official NYC TLC taxi zone lookup CSV

    Grain: 1 row = 1 taxi zone location
    Columns: location_id, borough, zone, service_zone
    """
    print("\n--- Building dim_taxi_zone ---")
    print(f"Reading taxi zone lookup from: {zone_lookup_path}")

    try:
        dim_taxi_zone = spark.read.csv(zone_lookup_path, header=True, inferSchema=True) \
            .select(
                col("LocationID").alias("location_id"),
                col("Borough").alias("borough"),
                col("Zone").alias("zone"),
                col("service_zone")
            )
    except Exception as e:
        raise ValueError(f"Failed to read taxi zone lookup from {zone_lookup_path}. Error: {str(e)}")

    zone_count = dim_taxi_zone.count()
    print(f"dim_taxi_zone records: {zone_count}")
    print("Sample zones:")
    dim_taxi_zone.show(10, truncate=False)

    return dim_taxi_zone


def transform_to_fact(df_cleaned: DataFrame) -> DataFrame:
    """
    Transform cleaned data to fact_taxi_trip

    Grain: 1 row = 1 taxi trip
    """
    print("\n--- Building fact_taxi_trip ---")

    # Generate trip_id
    df_fact = df_cleaned \
        .withColumn("trip_id", monotonically_increasing_id())

    # Calculate derived metrics
    df_fact = df_fact \
        .withColumn("tip_rate",
                   when(col("total_amount") > 0,
                        spark_round(col("tip_amount") / col("total_amount"), 4))
                   .otherwise(lit(0.0))) \
        .withColumn("fare_per_mile",
                   when(col("trip_distance") > 0,
                        spark_round(col("fare_amount") / col("trip_distance"), 2))
                   .otherwise(lit(0.0))) \
        .withColumn("fare_per_minute",
                   when(col("trip_duration_min") > 0,
                        spark_round(col("fare_amount") / col("trip_duration_min"), 2))
                   .otherwise(lit(0.0)))

    # Select fact columns
    fact_columns = [
        "trip_id",
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "year_month",
        "pickup_year",
        "pickup_month",
        "pickup_day",
        "pickup_hour",
        "pickup_dayofweek",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "ratecode_id",
        "passenger_count",
        "trip_distance",
        "trip_duration_sec",
        "trip_duration_min",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "extra",
        "mta_tax",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        "tip_rate",
        "fare_per_mile",
        "fare_per_minute",
        "created_at"
    ]

    # Add year/month for partitioning
    df_fact_final = df_fact.select(*fact_columns) \
        .withColumn("year", col("pickup_year")) \
        .withColumn("month", col("pickup_month"))

    fact_count = df_fact_final.count()
    print(f"fact_taxi_trip records: {fact_count:,}")

    # Show sample
    print("\nSample fact_taxi_trip:")
    df_fact_final.select(
        "trip_id", "vendor_id", "pickup_datetime",
        "pickup_location_id", "dropoff_location_id",
        "trip_distance", "trip_duration_min",
        "total_amount", "tip_rate", "fare_per_mile"
    ).show(5, truncate=False)

    return df_fact_final


def write_fact_table(df_fact: DataFrame, output_path: str):
    """
    Write fact_taxi_trip to warehouse layer
    Partitioned by year and month
    """
    print(f"\nWriting fact_taxi_trip to: {output_path}")

    df_fact.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print("fact_taxi_trip saved successfully!")
    print("Partitioned by: year, month")


def write_dimension_table(df_dim: DataFrame, output_path: str, dim_name: str):
    """
    Write dimension table (no partitioning)
    """
    print(f"\nWriting {dim_name} to: {output_path}")

    df_dim.write \
        .mode("overwrite") \
        .parquet(output_path)

    print(f"{dim_name} saved successfully!")


def build_and_write_dim_vendor(df_cleaned: DataFrame, output_path: str) -> DataFrame:
    """
    Build dim_vendor and write it to the warehouse layer
    """
    dim_vendor = build_dim_vendor(df_cleaned)
    write_dimension_table(dim_vendor, output_path, "dim_vendor")
    return dim_vendor


def build_and_write_dim_taxi_zone(spark, zone_lookup_path: str, output_path: str) -> DataFrame:
    """
    Build dim_taxi_zone and write it to the warehouse layer
    """
    dim_taxi_zone = build_dim_taxi_zone(spark, zone_lookup_path)
    write_dimension_table(dim_taxi_zone, output_path, "dim_taxi_zone")
    return dim_taxi_zone
