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
from pyspark.sql.types import DoubleType, LongType

VALID_TAXI_TYPES = ("yellow", "green", "fhvhv", "fhv")

# EXP-01 (Column Pruning, docs/spark_tuning_plan.md): the exact source
# columns each _transform_* function references, EXCLUDING the "year"/
# "month" helper columns save_raw_layer.py adds to the Raw layer for its
# own partitioning (Cleaned re-derives its own pickup_year/pickup_month
# from the timestamp, so those two raw-only columns are never used here).
# Passed to read_raw_data's select_columns so the projection is pushed
# down to the Parquet scan before any .count()/.filter() action, instead
# of happening only at the final .select() inside _transform_*.
REQUIRED_SOURCE_COLUMNS = {
    "yellow": [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "PULocationID", "DOLocationID", "RatecodeID", "payment_type",
        "store_and_fwd_flag", "passenger_count", "trip_distance",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "improvement_surcharge", "total_amount", "congestion_surcharge",
        "Airport_fee", "cbd_congestion_fee",
    ],
    "green": [
        "VendorID", "lpep_pickup_datetime", "lpep_dropoff_datetime",
        "PULocationID", "DOLocationID", "RatecodeID", "payment_type",
        "store_and_fwd_flag", "trip_type", "passenger_count", "trip_distance",
        "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
        "ehail_fee", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "cbd_congestion_fee",
    ],
    "fhvhv": [
        "hvfhs_license_num", "dispatching_base_num", "originating_base_num",
        "PULocationID", "DOLocationID", "request_datetime", "on_scene_datetime",
        "pickup_datetime", "dropoff_datetime", "trip_miles", "trip_time",
        "base_passenger_fare", "tolls", "bcf", "sales_tax",
        "congestion_surcharge", "airport_fee", "tips", "driver_pay",
        "cbd_congestion_fee", "shared_request_flag", "shared_match_flag",
        "wav_request_flag", "wav_match_flag",
    ],
    "fhv": [
        "dispatching_base_num", "Affiliated_base_number",
        "PUlocationID", "DOlocationID", "pickup_datetime", "dropOff_datetime",
        "SR_Flag",
    ],
}



def read_raw_data(spark, input_path, select_columns=None):
    """
    Read raw NYC taxi data from parquet/csv
    Supports local filesystem, S3, GCS, HDFS

    input_path may be a single path string, or a list of paths (e.g. the
    specific year=/month= partition paths for the months this run is
    processing - see collection_range.raw_layer_month_paths). Scoping the
    read to specific partitions, rather than the whole raw layer directory,
    matters here: Spark otherwise has to merge Parquet schemas across every
    month ever written to that path, and different write runs can produce
    incompatible physical encodings for the same logical column (observed
    with congestion_surcharge: double in one month's file, INT32 in
    another's - see docs/known_issues_and_fixes.md).

    select_columns (EXP-01, Column Pruning): optional list of column names
    to keep, applied immediately after the read and before the validation
    .count() - so the projection reaches the Parquet scan (and every
    downstream action, including the .count()/.filter() calls inside
    transform_to_cleaned) instead of only being applied at the final
    .select() inside _transform_*. Columns not present in this particular
    file are silently skipped rather than erroring, since some fee columns
    (e.g. cbd_congestion_fee) don't exist in older months - see
    _with_optional_double_columns, which still backfills those afterward.
    """
    paths = input_path if isinstance(input_path, list) else [input_path]
    print(f"Reading raw data from ({len(paths)} path(s)): {paths}")

    first_path = paths[0]
    # Determine storage type
    if first_path.startswith("s3://") or first_path.startswith("s3a://"):
        storage_type = "AWS S3"
    elif first_path.startswith("gs://"):
        storage_type = "Google Cloud Storage"
    elif first_path.startswith("hdfs://"):
        storage_type = "HDFS"
    else:
        storage_type = "Local filesystem"

    print(f"Storage type: {storage_type}")

    # Read data
    try:
        df = spark.read.parquet(*paths)
        print(f"Successfully loaded parquet from {storage_type}")
    except Exception as e:
        print(f"Failed to read as parquet: {str(e)}")
        try:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(*paths)
            print(f"Successfully loaded CSV from {storage_type}")
        except Exception as e:
            raise ValueError(f"Cannot read data from {paths}. Error: {str(e)}")

    # Validate
    if df is None:
        raise ValueError(f"Failed to load data from {paths}")

    if select_columns is not None:
        available = [c for c in select_columns if c in df.columns]
        dropped = [c for c in df.columns if c not in available]
        print(f"Column pruning: keeping {len(available)}/{len(df.columns)} columns, dropping {dropped}")
        df = df.select(*available)

    record_count = df.count()
    if record_count == 0:
        raise ValueError(f"Input data is empty (0 records) from {paths}")

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


def _with_optional_double_columns(df: DataFrame, column_names) -> DataFrame:
    """
    Some fee columns (e.g. cbd_congestion_fee, added for NYC's Jan 2025
    congestion pricing) don't exist in older monthly files even though TLC
    has otherwise backfilled the modern schema (VendorID, PULocationID, etc.)
    back to ~2011. Add them as null so older months don't fail the select
    below just because a fee that didn't exist yet is missing.
    """
    for name in column_names:
        if name not in df.columns:
            df = df.withColumn(name, lit(None).cast(DoubleType()))
    return df


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


def _round_double(column_name: str, decimals: int = 2):
    """
    spark_round(col(x), n) preserves col(x)'s existing type instead of
    normalizing to double - so if TLC's source parquet happens to encode a
    fee column as INT32 for some months (observed with congestion_surcharge,
    likely because every value in that month was a whole number) and as
    DOUBLE for others, that inconsistency survives straight into our own
    Cleaned Layer output. Once enough months accumulate, reading them back
    together (Fact/Dim, or Cleaned itself before the per-run read scoping)
    fails with a Parquet physical-type mismatch. Casting to DoubleType
    first guarantees every month's cleaned output has the same physical
    type for these columns, regardless of what the source happened to use.
    """
    return spark_round(col(column_name).cast(DoubleType()), decimals)


def _cast_long(column_name: str):
    """
    Same rationale as _round_double, for integer-typed ID/count columns
    (vendor_id, location IDs, ratecode_id, payment_type, passenger_count).
    Observed directly: vendor_id physically encoded as INT32 in one 2024
    month's source file and as bigint (LongType) in others, which crashes
    Fact-layer reads that combine months across a full year the same way
    congestion_surcharge did (see docs/known_issues_and_fixes.md). Casting
    to LongType at Cleaned Layer build time gives every month's output a
    stable physical type regardless of what a given month's source used.
    """
    return col(column_name).cast(LongType())


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
    df_time = _with_optional_double_columns(df_time, ["cbd_congestion_fee"])

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
        _cast_long("VendorID").alias("vendor_id"),
        _cast_long("PULocationID").alias("pickup_location_id"),
        _cast_long("DOLocationID").alias("dropoff_location_id"),
        _cast_long("RatecodeID").alias("ratecode_id"),
        _cast_long("payment_type").alias("payment_type"),
        col("store_and_fwd_flag"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        _cast_long("passenger_count").alias("passenger_count"),
        _round_double("trip_distance").alias("trip_distance"),
        col("trip_duration_sec"), col("trip_duration_min"),
        _round_double("fare_amount").alias("fare_amount"),
        _round_double("extra").alias("extra"),
        _round_double("mta_tax").alias("mta_tax"),
        _round_double("tip_amount").alias("tip_amount"),
        _round_double("tolls_amount").alias("tolls_amount"),
        _round_double("improvement_surcharge").alias("improvement_surcharge"),
        _round_double("total_amount").alias("total_amount"),
        _round_double("congestion_surcharge").alias("congestion_surcharge"),
        _round_double("Airport_fee").alias("airport_fee"),
        _round_double("cbd_congestion_fee").alias("cbd_congestion_fee"),
        current_timestamp().alias("created_at"),
    )


def _transform_green(df: DataFrame) -> DataFrame:
    """green: same fare/passenger schema as yellow, lpep_* datetime columns, plus trip_type/ehail_fee"""
    df_time = _with_time_features(df, "lpep_pickup_datetime", "lpep_dropoff_datetime")
    df_time = _with_optional_double_columns(df_time, ["cbd_congestion_fee"])

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
        _cast_long("VendorID").alias("vendor_id"),
        _cast_long("PULocationID").alias("pickup_location_id"),
        _cast_long("DOLocationID").alias("dropoff_location_id"),
        _cast_long("RatecodeID").alias("ratecode_id"),
        _cast_long("payment_type").alias("payment_type"),
        col("store_and_fwd_flag"),
        col("trip_type"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        _cast_long("passenger_count").alias("passenger_count"),
        _round_double("trip_distance").alias("trip_distance"),
        col("trip_duration_sec"), col("trip_duration_min"),
        _round_double("fare_amount").alias("fare_amount"),
        _round_double("extra").alias("extra"),
        _round_double("mta_tax").alias("mta_tax"),
        _round_double("tip_amount").alias("tip_amount"),
        _round_double("tolls_amount").alias("tolls_amount"),
        _round_double("ehail_fee").alias("ehail_fee"),
        _round_double("improvement_surcharge").alias("improvement_surcharge"),
        _round_double("total_amount").alias("total_amount"),
        _round_double("congestion_surcharge").alias("congestion_surcharge"),
        _round_double("cbd_congestion_fee").alias("cbd_congestion_fee"),
        current_timestamp().alias("created_at"),
    )


def _transform_fhvhv(df: DataFrame) -> DataFrame:
    """fhvhv: no VendorID/passenger_count; fare split across base_passenger_fare/tolls/bcf/sales_tax/tips/driver_pay"""
    df_time = _with_time_features(df, "pickup_datetime", "dropoff_datetime")
    df_time = _with_optional_double_columns(df_time, ["cbd_congestion_fee"])

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
        _cast_long("PULocationID").alias("pickup_location_id"),
        _cast_long("DOLocationID").alias("dropoff_location_id"),
        col("request_datetime"),
        col("on_scene_datetime"),
        col("pickup_datetime"),
        col("dropoff_datetime"),
        col("pickup_date"), col("year_month"), col("pickup_year"), col("pickup_month"),
        col("pickup_day"), col("pickup_hour"), col("pickup_dayofweek"),
        _round_double("trip_miles").alias("trip_distance"),
        col("trip_time"),
        col("trip_duration_sec"), col("trip_duration_min"),
        _round_double("base_passenger_fare").alias("base_passenger_fare"),
        _round_double("tolls").alias("tolls"),
        _round_double("bcf").alias("bcf"),
        _round_double("sales_tax").alias("sales_tax"),
        _round_double("congestion_surcharge").alias("congestion_surcharge"),
        _round_double("airport_fee").alias("airport_fee"),
        _round_double("tips").alias("tips"),
        _round_double("driver_pay").alias("driver_pay"),
        _round_double("cbd_congestion_fee").alias("cbd_congestion_fee"),
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
        _cast_long("PUlocationID").alias("pickup_location_id"),
        _cast_long("DOlocationID").alias("dropoff_location_id"),
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
