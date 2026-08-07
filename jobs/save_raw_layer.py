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
