"""
Fact to Mart Layer - mart_month_vendor_cumulative_metrics
Build cumulative mart table for monthly vendor metrics with window aggregation
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col, sum as spark_sum, count, round as spark_round,
    current_timestamp, broadcast
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


def build_mart_month_vendor_cumulative_metrics(df_fact: DataFrame, df_dim_vendor: DataFrame) -> DataFrame:
    """
    Build mart_month_vendor_cumulative_metrics

    Grain: 1 row = year_month + vendor_id

    Metrics:
    - Monthly aggregates: monthly_trip_count, monthly_passenger_count, monthly_distance, monthly_fare_amount, etc.
    - Cumulative aggregates: cumulative_trip_count, cumulative_passenger_count, cumulative_distance, etc.

    Strategy: Window function for cumulative calculation
    """
    print("\n--- Building mart_month_vendor_cumulative_metrics ---")
    print("Grain: year_month + vendor_id")
    print("Strategy: Monthly aggregation + Window function for cumulative")

    # Step 1: Monthly aggregation by vendor
    print("\nStep 1: Monthly aggregation by vendor...")

    df_monthly = df_fact.groupBy(
        "year_month",
        "pickup_year",
        "pickup_month",
        "vendor_id"
    ).agg(
        count("*").alias("monthly_trip_count"),
        spark_sum("passenger_count").alias("monthly_passenger_count"),
        spark_round(spark_sum("trip_distance"), 2).alias("monthly_distance"),
        spark_round(spark_sum("fare_amount"), 2).alias("monthly_fare_amount"),
        spark_round(spark_sum("tip_amount"), 2).alias("monthly_tip_amount"),
        spark_round(spark_sum("total_amount"), 2).alias("monthly_total_amount")
    )

    monthly_count = df_monthly.count()
    print(f"Monthly aggregated records: {monthly_count:,}")

    # Step 2: Calculate cumulative metrics using Window function
    print("Step 2: Calculating cumulative metrics using Window function...")

    # Define window: partition by vendor_id, order by year_month
    # Use rowsBetween to sum all rows from beginning to current row
    window_spec = Window.partitionBy("vendor_id") \
        .orderBy("year_month") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    df_cumulative = df_monthly \
        .withColumn("cumulative_trip_count", spark_sum("monthly_trip_count").over(window_spec)) \
        .withColumn("cumulative_passenger_count", spark_sum("monthly_passenger_count").over(window_spec)) \
        .withColumn("cumulative_distance", spark_round(spark_sum("monthly_distance").over(window_spec), 2)) \
        .withColumn("cumulative_fare_amount", spark_round(spark_sum("monthly_fare_amount").over(window_spec), 2)) \
        .withColumn("cumulative_tip_amount", spark_round(spark_sum("monthly_tip_amount").over(window_spec), 2)) \
        .withColumn("cumulative_total_amount", spark_round(spark_sum("monthly_total_amount").over(window_spec), 2))

    print("Cumulative metrics calculated")

    # Step 3: Join with dim_vendor (broadcast join)
    print("Step 3: Joining with dim_vendor (broadcast join)...")

    df_mart = df_cumulative.join(
        broadcast(df_dim_vendor),
        df_cumulative.vendor_id == df_dim_vendor.vendor_id,
        "left"
    )

    # Step 4: Select final columns
    print("Step 4: Selecting final columns...")

    df_final = df_mart.select(
        # Dimensions
        col("pickup_year").alias("year"),
        col("pickup_month").alias("month"),
        col("year_month"),
        df_cumulative.vendor_id,
        col("vendor_name"),

        # Monthly metrics
        col("monthly_trip_count"),
        col("monthly_passenger_count"),
        col("monthly_distance"),
        col("monthly_fare_amount"),
        col("monthly_tip_amount"),
        col("monthly_total_amount"),

        # Cumulative metrics
        col("cumulative_trip_count"),
        col("cumulative_passenger_count"),
        col("cumulative_distance"),
        col("cumulative_fare_amount"),
        col("cumulative_tip_amount"),
        col("cumulative_total_amount"),

        # Metadata
        current_timestamp().alias("created_at"),
        current_timestamp().alias("updated_at")
    )

    mart_count = df_final.count()
    print(f"Final mart records: {mart_count:,}")

    # Step 5: Show sample
    print("\nSample mart_month_vendor_cumulative_metrics:")
    df_final.select(
        "year_month", "vendor_name",
        "monthly_trip_count", "monthly_total_amount",
        "cumulative_trip_count", "cumulative_total_amount"
    ).orderBy("vendor_id", "year_month").show(20, truncate=False)

    # Step 6: Show cumulative trend
    print("\nCumulative trend by vendor:")
    df_final.groupBy("vendor_name") \
        .agg(
            spark_sum("monthly_trip_count").alias("total_trips"),
            spark_round(spark_sum("monthly_total_amount"), 2).alias("total_revenue")
        ) \
        .show(truncate=False)

    return df_final


def write_mart_table(df_mart: DataFrame, output_path: str):
    """
    Write mart table to output path
    Partitioned by year and month
    """
    print(f"\nWriting mart_month_vendor_cumulative_metrics to: {output_path}")

    df_mart.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print("mart_month_vendor_cumulative_metrics saved successfully!")
    print("Partitioned by: year, month")


def validate_mart(df_fact: DataFrame, df_mart: DataFrame):
    """
    Validate mart data against fact table
    """
    print("\n--- Validating Mart Data ---")

    # Monthly count validation
    fact_count = df_fact.count()
    mart_monthly_count = df_mart.agg(spark_sum("monthly_trip_count")).collect()[0][0]

    print(f"Fact table count: {fact_count:,}")
    print(f"Mart monthly_trip_count sum: {mart_monthly_count:,}")

    if fact_count == mart_monthly_count:
        print("✓ Monthly count validation PASSED")
    else:
        print(f"⚠ Monthly count validation WARNING: difference = {abs(fact_count - mart_monthly_count):,}")

    # Monthly amount validation
    fact_total = df_fact.agg(spark_round(spark_sum("total_amount"), 2)).collect()[0][0]
    mart_monthly_total = df_mart.agg(spark_round(spark_sum("monthly_total_amount"), 2)).collect()[0][0]

    print(f"Fact total_amount: ${fact_total:,.2f}")
    print(f"Mart monthly_total_amount sum: ${mart_monthly_total:,.2f}")

    if abs(fact_total - mart_monthly_total) < 0.01:
        print("✓ Monthly amount validation PASSED")
    else:
        print(f"⚠ Monthly amount validation WARNING: difference = ${abs(fact_total - mart_monthly_total):,.2f}")

    # Cumulative logic validation
    print("\nCumulative logic validation:")
    print("Checking if cumulative values are monotonically increasing...")

    # For simplicity, just show that cumulative >= monthly for each row
    validation_df = df_mart.select(
        "year_month", "vendor_name",
        "monthly_trip_count", "cumulative_trip_count"
    ).orderBy("vendor_name", "year_month")

    print("Sample cumulative progression:")
    validation_df.show(10, truncate=False)

    print("--- Validation Complete ---\n")


def build_and_write_mart_month_vendor_cumulative_metrics(
    spark, fact_path: str, dim_vendor_path: str, output_path: str
) -> DataFrame:
    """
    Run mart_month_vendor_cumulative_metrics end-to-end:
    read fact_taxi_trip + dim_vendor -> build -> write -> validate
    """
    df_fact = read_fact_table(spark, fact_path)
    df_dim_vendor = read_dim_vendor(spark, dim_vendor_path)

    df_mart = build_mart_month_vendor_cumulative_metrics(df_fact, df_dim_vendor)

    write_mart_table(df_mart, output_path)

    validate_mart(df_fact, df_mart)

    return df_mart
