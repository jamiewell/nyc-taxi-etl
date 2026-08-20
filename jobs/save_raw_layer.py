"""
Save Raw Layer
Preserve source data to Raw Layer without transformation
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month

# Pickup datetime column name differs by taxi type; used only to derive the
# year/month partition columns for the raw layer (source data itself is not
# renamed or otherwise modified).
PICKUP_DATETIME_COLUMN = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhvhv": "pickup_datetime",
    "fhv": "pickup_datetime",
}


def read_source_data(spark, input_path):
    """
    Read source data from external location (local, S3, etc)

    input_path may be a single path string, or a list of paths (e.g. the
    specific year=/month= partition paths resolved by
    collection_range.resolve_year_month_paths for a --start-year-month /
    --end-year-month range). All paths in a list must be readable in a
    single format (parquet or CSV, not mixed).
    """
    paths = input_path if isinstance(input_path, list) else [input_path]
    print(f"Reading source data from ({len(paths)} path(s)): {paths}")

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

    record_count = df.count()
    if record_count == 0:
        raise ValueError(f"Source data is empty (0 records) from {paths}")

    print(f"Source records: {record_count:,}")
    df.printSchema()

    return df


def save_to_raw_layer(df: DataFrame, output_path: str, taxi_type: str = "yellow"):
    """
    Save data to Raw Layer with year/month partition
    No transformation, preserve original data
    """
    if taxi_type not in PICKUP_DATETIME_COLUMN:
        raise ValueError(
            f"Unknown taxi_type: {taxi_type!r}. Must be one of {sorted(PICKUP_DATETIME_COLUMN)}"
        )

    print(f"\nSaving to Raw Layer: {output_path} (taxi_type={taxi_type})")

    pickup_col = PICKUP_DATETIME_COLUMN[taxi_type]

    # Add partition columns for storage
    df_with_partition = df \
        .withColumn("year", year(pickup_col)) \
        .withColumn("month", month(pickup_col))

    # Write to raw layer
    df_with_partition.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print("Raw Layer saved successfully!")
    print("Partitioned by: year, month")
    print("Raw data preserved without modification")

def save_month_to_raw_layer(df: DataFrame, output_path: str, taxi_type: str,
                             year_value: int, month_value: int):
    """
    Writes ALL rows of df (one source month's file, completely unfiltered)
    to an explicit partition path: <output_path>/year=<year_value>/month=<month_value>.

    This exists because save_to_raw_layer's dynamic, per-row pickup-date
    partitioning is unsafe when called in a loop across consecutive months
    (main.py's --start-year-month/--end-year-month range mode): source
    files aren't perfectly clean - e.g. the 2024-02 file contains a small
    number of rows whose actual pickup_datetime falls in January. Under
    dynamic partition overwrite, writing that file would silently
    overwrite the much larger, already-written year=2024/month=1 partition
    with just those handful of stray rows. Confirmed directly: processing
    2024-01..12 in one run left only the last month (December) with real
    data; every earlier month's partition had been wiped down to a few KB
    by a later month's stragglers. See docs/known_issues_and_fixes.md.

    Writing to an explicit path keyed by the source file's own nominal
    year/month avoids the collision entirely (each month's write only ever
    touches its own directory), and every row from the source is kept - a
    trip whose pickup_datetime lands in the "wrong" month is filed under
    the source file's nominal month rather than dropped, consistent with
    "raw data is not modified".
    """
    if taxi_type not in PICKUP_DATETIME_COLUMN:
        raise ValueError(
            f"Unknown taxi_type: {taxi_type!r}. Must be one of {sorted(PICKUP_DATETIME_COLUMN)}"
        )

    # Not zero-padded, to match collection_range.raw_layer_month_paths and
    # how Spark's own partitionBy names directories elsewhere in this project.
    target_path = f"{output_path.rstrip('/')}/year={year_value}/month={month_value}"
    print(f"\nSaving to Raw Layer (explicit path): {target_path} (taxi_type={taxi_type})")

    df.write.mode("overwrite").parquet(target_path)

    print("Raw Layer saved successfully!")
    print("Written to source file's nominal year/month (not per-row pickup date) - no dynamic partition collision risk")
    print("Raw data preserved without modification")

