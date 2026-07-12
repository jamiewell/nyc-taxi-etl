"""
Raw to Cleaned Layer ETL Job
Transform raw NYC taxi data to cleaned layer with quality filters and standard columns
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, to_date, year, month, dayofmonth, hour, dayofweek,
    round as spark_round, unix_timestamp, current_timestamp,
    concat_ws
)


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


def transform_to_cleaned(df: DataFrame) -> DataFrame:
    """
    Transform raw data to cleaned layer

    Transformations:
    1. Column name standardization
    2. Time feature extraction
    3. Data quality filters
    4. Derived columns
    """
    print("\n=== Starting Cleaned Layer Transformation ===")

    # Step 1: Extract time features
    print("Step 1: Extracting time features...")
    df_with_time = df \
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_day", dayofmonth(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime"))) \
        .withColumn("year_month", concat_ws("-",
                                            year(col("tpep_pickup_datetime")),
                                            month(col("tpep_pickup_datetime"))))

    # Step 2: Calculate trip duration
    print("Step 2: Calculating trip duration...")
    df_with_duration = df_with_time \
        .withColumn("trip_duration_sec",
                   unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) \
        .withColumn("trip_duration_min",
                   spark_round((unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0, 2))

    # Step 3: Apply data quality filters
    print("Step 3: Applying data quality filters...")

    initial_count = df_with_duration.count()

    df_filtered = df_with_duration.filter(
        # Basic null checks
        (col("tpep_pickup_datetime").isNotNull()) &
        (col("tpep_dropoff_datetime").isNotNull()) &

        # Time validation
        (col("tpep_dropoff_datetime") > col("tpep_pickup_datetime")) &
        (col("trip_duration_min") > 0) &
        (col("trip_duration_min") <= 1440) &  # Max 24 hours

        # Location validation
        (col("PULocationID").isNotNull()) &
        (col("DOLocationID").isNotNull()) &

        # Business logic validation
        (col("passenger_count") > 0) &
        (col("passenger_count") <= 8) &  # Reasonable max passengers
        (col("trip_distance") > 0) &
        (col("trip_distance") <= 200) &  # Reasonable max distance in miles
        (col("fare_amount") > 0) &
        (col("total_amount") > 0)
    )

    filtered_count = df_filtered.count()
    filtered_out = initial_count - filtered_count
    filter_rate = (filtered_count / initial_count * 100) if initial_count > 0 else 0

    print(f"  Initial records: {initial_count:,}")
    print(f"  After filtering: {filtered_count:,}")
    print(f"  Filtered out: {filtered_out:,} ({100-filter_rate:.2f}%)")
    print(f"  Pass rate: {filter_rate:.2f}%")

    # Step 4: Standardize column names and select columns
    print("Step 4: Selecting and standardizing columns...")

    df_cleaned = df_filtered.select(
        # IDs
        col("VendorID").alias("vendor_id"),
        col("PULocationID").alias("pickup_location_id"),
        col("DOLocationID").alias("dropoff_location_id"),
        col("RatecodeID").alias("ratecode_id"),
        col("payment_type"),
        col("store_and_fwd_flag"),

        # Timestamps
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),

        # Time features
        col("pickup_date"),
        col("year_month"),
        col("pickup_year"),
        col("pickup_month"),
        col("pickup_day"),
        col("pickup_hour"),
        col("pickup_dayofweek"),

        # Trip metrics
        col("passenger_count"),
        spark_round(col("trip_distance"), 2).alias("trip_distance"),
        col("trip_duration_sec"),
        col("trip_duration_min"),

        # Financial metrics
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

        # Metadata
        current_timestamp().alias("created_at")
    )

    # Step 5: Show sample
    print("\nSample cleaned data:")
    df_cleaned.select(
        "pickup_date", "pickup_hour", "pickup_dayofweek",
        "vendor_id", "pickup_location_id", "dropoff_location_id",
        "passenger_count", "trip_distance", "trip_duration_min",
        "fare_amount", "total_amount"
    ).show(10, truncate=False)

    print(f"\n=== Cleaned Transformation Complete ===")
    print(f"Output records: {filtered_count:,}")

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
