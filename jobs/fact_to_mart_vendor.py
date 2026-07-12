"""
Fact to Mart Layer - mart_month_hour_vendor_trip_metrics
Build mart table for month/hour/vendor trip metrics with dimension join
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, round as spark_round,
    current_timestamp, expr, when, lit, broadcast
)


def read_fact_table(spark, fact_path):
    """
    Read fact_taxi_trip from warehouse layer
    """
    print(f"Reading fact_taxi_trip from: {fact_path}")

    try:
        df = spark.read.parquet(fact_path)
        record_count = df.count()
        print(f"Fact records loaded: {record_count:,}")
        return df
    except Exception as e:
        raise ValueError(f"Failed to read fact table from {fact_path}. Error: {str(e)}")


def read_dim_vendor(spark, dim_path):
    """
    Read dim_vendor dimension table
    """
    print(f"Reading dim_vendor from: {dim_path}")

    try:
        df = spark.read.parquet(dim_path)
        vendor_count = df.count()
        print(f"dim_vendor records loaded: {vendor_count}")
        return df
    except Exception as e:
        raise ValueError(f"Failed to read dim_vendor from {dim_path}. Error: {str(e)}")


def build_mart_month_hour_vendor_trip_metrics(df_fact: DataFrame, df_dim_vendor: DataFrame) -> DataFrame:
    """
    Build mart_month_hour_vendor_trip_metrics

    Grain: 1 row = year_month + pickup_hour + vendor_id

    Aggregations:
    - trip_count, total_passenger_count
    - total_distance, avg_distance
    - total_duration_min, avg_duration_min, p50_duration_min, p90_duration_min
    - total_fare_amount, total_tip_amount, total_amount
    - avg_fare_amount, avg_total_amount
    - fare_per_minute, fare_per_mile
    """
    print("\n--- Building mart_month_hour_vendor_trip_metrics ---")
    print("Grain: year_month + pickup_hour + vendor_id")

    # Step 1: GroupBy and Aggregate
    print("\nStep 1: GroupBy aggregation by vendor...")

    df_agg = df_fact.groupBy(
        "year_month",
        "pickup_year",
        "pickup_month",
        "pickup_hour",
        "vendor_id"
    ).agg(
        # Trip metrics
        count("*").alias("trip_count"),
        spark_sum("passenger_count").alias("total_passenger_count"),

        # Distance metrics
        spark_round(spark_sum("trip_distance"), 2).alias("total_distance"),
        spark_round(avg("trip_distance"), 2).alias("avg_distance"),

        # Duration metrics
        spark_round(spark_sum("trip_duration_min"), 2).alias("total_duration_min"),
        spark_round(avg("trip_duration_min"), 2).alias("avg_duration_min"),
        expr("percentile_approx(trip_duration_min, 0.5)").alias("p50_duration_min"),
        expr("percentile_approx(trip_duration_min, 0.9)").alias("p90_duration_min"),

        # Financial metrics
        spark_round(spark_sum("fare_amount"), 2).alias("total_fare_amount"),
        spark_round(spark_sum("tip_amount"), 2).alias("total_tip_amount"),
        spark_round(spark_sum("total_amount"), 2).alias("total_amount"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
        spark_round(avg("total_amount"), 2).alias("avg_total_amount")
    )

    agg_count = df_agg.count()
    print(f"Aggregated records: {agg_count:,}")

    # Step 2: Calculate derived metrics
    print("Step 2: Calculating derived metrics...")

    df_with_derived = df_agg \
        .withColumn("fare_per_minute",
                   when(col("total_duration_min") > 0,
                        spark_round(col("total_amount") / col("total_duration_min"), 2))
                   .otherwise(lit(0.0))) \
        .withColumn("fare_per_mile",
                   when(col("total_distance") > 0,
                        spark_round(col("total_amount") / col("total_distance"), 2))
                   .otherwise(lit(0.0)))

    # Step 3: Join with dim_vendor (broadcast join for small dimension)
    print("Step 3: Joining with dim_vendor (broadcast join)...")

    df_mart = df_with_derived.join(
        broadcast(df_dim_vendor),
        df_with_derived.vendor_id == df_dim_vendor.vendor_id,
        "left"
    )

    # Step 4: Select final columns
    print("Step 4: Selecting final columns...")

    df_final = df_mart.select(
        # Dimensions
        col("pickup_year").alias("year"),
        col("pickup_month").alias("month"),
        col("year_month"),
        col("pickup_hour"),
        df_with_derived.vendor_id,
        col("vendor_name"),

        # Trip metrics
        col("trip_count"),
        col("total_passenger_count"),

        # Distance metrics
        col("total_distance"),
        col("avg_distance"),

        # Duration metrics
        col("total_duration_min"),
        col("avg_duration_min"),
        col("p50_duration_min"),
        col("p90_duration_min"),

        # Financial metrics
        col("total_fare_amount"),
        col("total_tip_amount"),
        col("total_amount"),
        col("avg_fare_amount"),
        col("avg_total_amount"),

        # Derived metrics
        col("fare_per_minute"),
        col("fare_per_mile"),

        # Metadata
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )

    mart_count = df_final.count()
    print(f"Final mart records: {mart_count:,}")

    # Step 5: Show sample
    print("\nSample mart_month_hour_vendor_trip_metrics:")
    df_final.select(
        "year_month", "pickup_hour", "vendor_id", "vendor_name",
        "trip_count", "total_distance", "avg_duration_min",
        "total_amount", "fare_per_mile"
    ).orderBy("year_month", "pickup_hour", "vendor_id").show(10, truncate=False)

    # Step 6: Show vendor comparison
    print("\nVendor comparison by hour:")
    df_final.groupBy("pickup_hour", "vendor_name") \
        .agg(
            spark_sum("trip_count").alias("total_trips"),
            spark_round(avg("avg_total_amount"), 2).alias("avg_fare")
        ) \
        .orderBy("pickup_hour", "vendor_name") \
        .show(20, truncate=False)

    return df_final


def write_mart_table(df_mart: DataFrame, output_path: str):
    """
    Write mart table to output path
    Partitioned by year and month
    """
    print(f"\nWriting mart_month_hour_vendor_trip_metrics to: {output_path}")

    df_mart.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print("mart_month_hour_vendor_trip_metrics saved successfully!")
    print("Partitioned by: year, month")


def validate_mart(df_fact: DataFrame, df_mart: DataFrame):
    """
    Validate mart data against fact table
    """
    print("\n--- Validating Mart Data ---")

    # Count validation
    fact_count = df_fact.count()
    mart_trip_count = df_mart.agg(spark_sum("trip_count")).collect()[0][0]

    print(f"Fact table count: {fact_count:,}")
    print(f"Mart trip_count sum: {mart_trip_count:,}")

    if fact_count == mart_trip_count:
        print("✓ Count validation PASSED")
    else:
        print(f"⚠ Count validation WARNING: difference = {abs(fact_count - mart_trip_count):,}")

    # Amount validation
    fact_total = df_fact.agg(spark_round(spark_sum("total_amount"), 2)).collect()[0][0]
    mart_total = df_mart.agg(spark_round(spark_sum("total_amount"), 2)).collect()[0][0]

    print(f"Fact total_amount: ${fact_total:,.2f}")
    print(f"Mart total_amount: ${mart_total:,.2f}")

    if abs(fact_total - mart_total) < 0.01:
        print("✓ Amount validation PASSED")
    else:
        print(f"⚠ Amount validation WARNING: difference = ${abs(fact_total - mart_total):,.2f}")

    print("--- Validation Complete ---\n")


def build_and_write_mart_month_hour_vendor_trip_metrics(
    spark, fact_path: str, dim_vendor_path: str, output_path: str
) -> DataFrame:
    """
    Run mart_month_hour_vendor_trip_metrics end-to-end:
    read fact_taxi_trip + dim_vendor -> build -> write -> validate
    """
    df_fact = read_fact_table(spark, fact_path)
    df_dim_vendor = read_dim_vendor(spark, dim_vendor_path)

    df_mart = build_mart_month_hour_vendor_trip_metrics(df_fact, df_dim_vendor)

    write_mart_table(df_mart, output_path)

    validate_mart(df_fact, df_mart)

    return df_mart
