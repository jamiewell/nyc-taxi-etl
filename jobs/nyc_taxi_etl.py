"""
NYC Taxi Data ETL Job
Transform functions for processing NYC taxi trip data
"""
from pyspark.sql.functions import col, to_date, hour, dayofweek, month, year, round as spark_round


def read_taxi_data(spark, input_path):
    """Read taxi data from parquet/csv (supports local, S3, GCS, HDFS)"""
    print(f"Reading data from: {input_path}")

    # Determine storage type
    if input_path.startswith("s3://") or input_path.startswith("s3a://"):
        storage_type = "AWS S3"
    elif input_path.startswith("gs://"):
        storage_type = "Google Cloud Storage"
    elif input_path.startswith("hdfs://"):
        storage_type = "HDFS"
    elif input_path.startswith("wasb://") or input_path.startswith("abfs://"):
        storage_type = "Azure Blob Storage"
    else:
        storage_type = "Local filesystem"

    print(f"Storage type detected: {storage_type}")

    df = None

    # Try reading as parquet first, fallback to csv
    try:
        df = spark.read.parquet(input_path)
        print(f"Successfully loaded data as parquet from {storage_type}")
    except Exception as e:
        print(f"Failed to read as parquet: {str(e)}")
        try:
            df = spark.read.option("header", "true").option("inferSchema", "true").csv(input_path)
            print(f"Successfully loaded data as CSV from {storage_type}")
        except Exception as e:
            print(f"Failed to read as CSV: {str(e)}")
            raise ValueError(f"Cannot read data from {input_path} ({storage_type}). Tried both parquet and CSV formats.")

    # Validate DataFrame
    if df is None:
        raise ValueError(f"Failed to load data from {input_path}")

    record_count = df.count()
    if record_count == 0:
        raise ValueError(f"Loaded data is empty (0 records) from {input_path}")

    print(f"Input records: {record_count}")
    df.printSchema()

    return df

def transform_taxi_data(df):
    """Apply transformations to taxi data"""
    print("Applying transformations...")

    # Convert pickup_datetime to date and extract time features
    df_transformed = df \
        .withColumn("pickup_date", to_date(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_dayofweek", dayofweek(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_month", month(col("tpep_pickup_datetime"))) \
        .withColumn("pickup_year", year(col("tpep_pickup_datetime"))) \
        .withColumn("total_amount_rounded", spark_round(col("total_amount"), 2))

    # Filter out invalid records
    df_clean = df_transformed.filter(
        (col("passenger_count") > 0) &
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0) &
        (col("total_amount") > 0)
    )

    print(f"Output records after cleaning: {df_clean.count()}")

    # Show sample data
    print("\nSample transformed data:")
    df_clean.select(
        "pickup_date", "pickup_hour", "pickup_dayofweek",
        "passenger_count", "trip_distance", "total_amount_rounded"
    ).show(10)

    # Show basic statistics
    print("\nBasic statistics:")
    df_clean.groupBy("pickup_hour").count().orderBy("pickup_hour").show()

    return df_clean

def write_taxi_data(df, output_path):
    """Write transformed data to parquet"""
    print(f"Writing data to: {output_path}")

    df.write \
        .mode("overwrite") \
        .partitionBy("pickup_year", "pickup_month") \
        .parquet(output_path)

    print("Write completed successfully!")

