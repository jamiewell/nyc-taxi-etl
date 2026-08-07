"""
Raw to Cleaned Layer ETL Job
Transform raw NYC taxi data to cleaned layer with quality filters and standard columns

NYC TLC publishes four taxi types with materially different schemas:
  - yellow / green: fare, passenger_count, trip_distance all present (only the
    pickup/dropoff datetime column names differ: tpep_* vs lpep_*)
  - fhvhv (High Volume For-Hire Vehicle): no VendorID/passenger_count; fare is
    split into base_passenger_fare/tolls/bcf/sales_tax/tips/driver_pay
  - fhv (For-Hire Vehicle): only dispatch/time/location columns - no fare,
    passenger_count, or trip_distance at all

Because the schemas diverge this much, each taxi type is transformed by its
own function and written to its own cleaned path (see build_cleaned_path in
main.py) rather than forced into one shared schema.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, lit, to_date, year, month, dayofmonth, hour, dayofweek,
    round as spark_round, unix_timestamp, current_timestamp,
    concat_ws
)

VALID_TAXI_TYPES = ("yellow", "green", "fhvhv", "fhv")


def read_raw_data(spark, input_path):
    """
    Read raw NYC taxi data from parquet/csv
    Supports local filesystem, S3, GCS, HDFS
    """
    print(f"Reading raw data from: {input_path}")

    # Determine storage type
    if input_path.startswith("s3://") or input_path.startswith("s3a://"):
        storage_type = "AWS S3"
    elif input_path.startswith("gs://"):
        storage_type = "Google Cloud Storage"
    elif input_path.startswith("hdfs://"):
        storage_type = "HDFS"
    else:
        storage_type = "Local filesystem"

    print(f"Storage type: {storage_type}")

    # Read data
    try:
        df = spark.read.parquet(input_path)
        print(f"Successfully loaded parquet from {storage_type}")
    except Exception as e:
        print(f"Failed to read as parquet: {str(e)}")
        try:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
            print(f"Successfully loaded CSV from {storage_type}")
        except Exception as e:
            raise ValueError(f"Cannot read data from {input_path}. Error: {str(e)}")

    # Validate
    if df is None:
        raise ValueError(f"Failed to load data from {input_path}")

    record_count = df.count()
    if record_count == 0:
        raise ValueError(f"Input data is empty (0 records) from {input_path}")

    print(f"Raw input records: {record_count:,}")
    df.printSchema()

    return df


def _with_time_features(df: DataFrame, pickup_col: str, dropoff_col: str) -> DataFrame:
    """Add the time features shared by every taxi type's cleaned schema."""
    return df \
        .withColumn("pickup_datetime", col(pickup_col)) \
        .withColumn("dropoff_datetime", col(dropoff_col)) \
        .withColumn("pickup_date", to_date(col(pickup_col))) \
        .withColumn("pickup_year", year(col(pickup_col))) \
        .withColumn("pickup_month", month(col(pickup_col))) \
        .withColumn("pickup_day", dayofmonth(col(pickup_col))) \
        .withColumn("pickup_hour", hour(col(pickup_col))) \
        .withColumn("pickup_dayofweek", dayofweek(col(pickup_col))) \
        .withColumn("year_month", concat_ws("-", year(col(pickup_col)), month(col(pickup_col)))) \
        .withColumn("trip_duration_sec", unix_timestamp(col(dropoff_col)) - unix_timestamp(col(pickup_col))) \
        .withColumn(
            "trip_duration_min",
            spark_round((unix_timestamp(col(dropoff_col)) - unix_timestamp(col(pickup_col))) / 60.0, 2)
        )


def _base_time_location_filter(df: DataFrame, pickup_col: str, dropoff_col: str,
                                pu_col: str, do_col: str) -> DataFrame:
    """Null/time/location validation shared by all taxi types."""
    return df.filter(
        (col(pickup_col).isNotNull()) &
        (col(dropoff_col).isNotNull()) &
        (col(dropoff_col) > col(pickup_col)) &
        (col("trip_duration_min") > 0) &
        (col("trip_duration_min") <= 1440) &  # Max 24 hours
        (col(pu_col).isNotNull()) &
        (col(do_col).isNotNull())
    )


def _log_filter_stats(initial_count: int, filtered_count: int) -> None:
    filtered_out = initial_count - filtered_count
    filter_rate = (filtered_count / initial_count * 100) if initial_count > 0 else 0
    print(f"  Initial records: {initial_count:,}")
    print(f"  After filtering: {filtered_count:,}")
    print(f"  Filtered out: {filtered_out:,} ({100 - filter_rate:.2f}%)")
    print(f"  Pass rate: {filter_rate:.2f}%")


def _transform_yellow(df: DataFrame) -> DataFrame:
    """yellow: full fare/passenger schema, tpep_* datetime columns"""
    df_time = _with_time_features(df, "tpep_pickup_datetime", "tpep_dropoff_datetime")

    initial_count = df_time.count()
    df_filtered = _base_time_location_filter(
        df_time, "tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"
    ).filter(
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 8) &
        (col("trip_distance") > 0) &
        (col("trip_distance") <= 200) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0)
    )
    filtered_count = df_filtered.count()
    _log_filter_stats(initial_count, filtered_count)

    return df_filtered.select(
        lit("yellow").alias("taxi_type"),
        col("VendorID").alias("vendor_id"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("RatecodeID").alias("ratecode_id"),
        col("payment_type"),
        col("store_and_fwd_flag"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        col("passenger_count"),
        spark_round(col("trip_distance"), 2).alias("trip_distance"),
        col("trip_duration_sec"), col("trip_duration_min"),
        spark_round(col("fare_amount"), 2).alias("fare_amount"),
        spark_round(col("extra"), 2).alias("extra"),
        spark_round(col("mta_tax"), 2).alias("mta_tax"),
        spark_round(col("tip_amount"), 2).alias("tip_amount"),
        spark_round(col("tolls_amount"), 2).alias("tolls_amount"),
        spark_round(col("improvement_surcharge"), 2).alias("improvement_surcharge"),
        spark_round(col("total_amount"), 2).alias("total_amount"),
        spark_round(col("congestion_surcharge"), 2).alias("congestion_surcharge"),
        spark_round(col("Airport_fee"), 2).alias("airport_fee"),
        spark_round(col("cbd_congestion_fee"), 2).alias("cbd_congestion_fee"),
        current_timestamp().alias("created_at"),
    )


def _transform_green(df: DataFrame) -> DataFrame:
    """green: same fare/passenger schema as yellow, lpep_* datetime columns, plus trip_type/ehail_fee"""
    df_time = _with_time_features(df, "lpep_pickup_datetime", "lpep_dropoff_datetime")

    initial_count = df_time.count()
    df_filtered = _base_time_location_filter(
        df_time, "lpep_pickup_datetime", "lpep_dropoff_datetime", "PULocationID", "DOLocationID"
    ).filter(
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 8) &
        (col("trip_distance") > 0) &
        (col("trip_distance") <= 200) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0)
    )
    filtered_count = df_filtered.count()
    _log_filter_stats(initial_count, filtered_count)

    return df_filtered.select(
        lit("green").alias("taxi_type"),
        col("VendorID").alias("vendor_id"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("RatecodeID").alias("ratecode_id"),
        col("payment_type"),
        col("store_and_fwd_flag"),
        col("trip_type"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        col("passenger_count"),
        spark_round(col("trip_distance"), 2).alias("trip_distance"),
        col("trip_duration_sec"), col("trip_duration_min"),
        spark_round(col("fare_amount"), 2).alias("fare_amount"),
        spark_round(col("extra"), 2).alias("extra"),
        spark_round(col("mta_tax"), 2).alias("mta_tax"),
        spark_round(col("tip_amount"), 2).alias("tip_amount"),
        spark_round(col("tolls_amount"), 2).alias("tolls_amount"),
        spark_round(col("ehail_fee"), 2).alias("ehail_fee"),
        spark_round(col("improvement_surcharge"), 2).alias("improvement_surcharge"),
        spark_round(col("total_amount"), 2).alias("total_amount"),
        spark_round(col("congestion_surcharge"), 2).alias("congestion_surcharge"),
        spark_round(col("cbd_congestion_fee"), 2).alias("cbd_congestion_fee"),
        current_timestamp().alias("created_at"),
    )


def _transform_fhvhv(df: DataFrame) -> DataFrame:
    """fhvhv: no VendorID/passenger_count; fare split across base_passenger_fare/tolls/bcf/sales_tax/tips/driver_pay"""
    df_time = _with_time_features(df, "pickup_datetime", "dropoff_datetime")

    initial_count = df_time.count()
    df_filtered = _base_time_location_filter(
        df_time, "pickup_datetime", "dropoff_datetime", "PULocationID", "DOLocationID"
    ).filter(
        (col("trip_miles") > 0) &
        (col("trip_miles") <= 200) &
        (col("base_passenger_fare") > 0)
    )
    filtered_count = df_filtered.count()
    _log_filter_stats(initial_count, filtered_count)

    return df_filtered.select(
        lit("fhvhv").alias("taxi_type"),
        col("hvfhs_license_num"),
        col("dispatching_base_num"),
        col("originating_base_num"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("request_datetime"),
        col("on_scene_datetime"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        spark_round(col("trip_miles"), 2).alias("trip_distance"),
        col("trip_time"),
        col("trip_duration_sec"), col("trip_duration_min"),
        spark_round(col("base_passenger_fare"), 2).alias("base_passenger_fare"),
        spark_round(col("tolls"), 2).alias("tolls"),
        spark_round(col("bcf"), 2).alias("bcf"),
        spark_round(col("sales_tax"), 2).alias("sales_tax"),
        spark_round(col("congestion_surcharge"), 2).alias("congestion_surcharge"),
        spark_round(col("airport_fee"), 2).alias("airport_fee"),
        spark_round(col("tips"), 2).alias("tips"),
        spark_round(col("driver_pay"), 2).alias("driver_pay"),
        spark_round(col("cbd_congestion_fee"), 2).alias("cbd_congestion_fee"),
        col("shared_request_flag"),
        col("shared_match_flag"),
        col("wav_request_flag"),
        col("wav_match_flag"),
        current_timestamp().alias("created_at"),
    )


def _transform_fhv(df: DataFrame) -> DataFrame:
    """fhv: dispatch/time/location only - no fare, passenger_count, or trip_distance"""
    df_time = _with_time_features(df, "pickup_datetime", "dropOff_datetime")

    initial_count = df_time.count()
    df_filtered = _base_time_location_filter(
        df_time, "pickup_datetime", "dropOff_datetime", "PUlocationID", "DOlocationID"
    )
    filtered_count = df_filtered.count()
    _log_filter_stats(initial_count, filtered_count)

    return df_filtered.select(
        lit("fhv").alias("taxi_type"),
        col("dispatching_base_num"),
        col("Affiliated_base_number").alias("affiliated_base_num"),
        col("PUlocationID").alias("pickup_location_id"),
        col("DOlocationID").alias("dropoff_location_id"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        col("trip_duration_sec"), col("trip_duration_min"),
        col("SR_Flag").alias("sr_flag"),
        current_timestamp().alias("created_at"),
    )


_TRANSFORM_BY_TAXI_TYPE = {
    "yellow": _transform_yellow,
    "green": _transform_green,
    "fhvhv": _transform_fhvhv,
    "fhv": _transform_fhv,
}


def transform_to_cleaned(df: DataFrame, taxi_type: str = "yellow") -> DataFrame:
    """
    Transform raw data to cleaned layer for the given taxi type.

    Each taxi type has its own quality filters and output schema (see module
    docstring) since yellow/green/fhvhv/fhv do not share a common set of
    fare/passenger/distance columns.
    """
    if taxi_type not in _TRANSFORM_BY_TAXI_TYPE:
        raise ValueError(f"Unknown taxi_type: {taxi_type!r}. Must be one of {VALID_TAXI_TYPES}")

    print(f"\n=== Starting Cleaned Layer Transformation (taxi_type={taxi_type}) ===")

    df_cleaned = _TRANSFORM_BY_TAXI_TYPE[taxi_type](df)

    print("\nSample cleaned data:")
    df_cleaned.show(10, truncate=False)

    print(f"\n=== Cleaned Transformation Complete (taxi_type={taxi_type}) ===")

    return df_cleaned


def write_cleaned_data(df: DataFrame, output_path: str):
    """
    Write cleaned data to parquet partitioned by year and month
    """
    print(f"\nWriting cleaned data to: {output_path}")

    # Write with year/month partition
    df.write \
        .mode("overwrite") \
        .partitionBy("pickup_year", "pickup_month") \
        .parquet(output_path)

    print("Write completed successfully!")
    print(f"Partitioned by: pickup_year, pickup_month")
