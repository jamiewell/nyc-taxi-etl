"""
Save Raw Layer
Preserve source data to Raw Layer without transformation
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import year, month


def read_source_data(spark, input_path):
    """
    Read source data from external location (local, S3, etc)
    """
    print(f"Reading source data from: {input_path}")

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
        raise ValueError(f"Source data is empty (0 records) from {input_path}")

    print(f"Source records: {record_count:,}")
    df.printSchema()

    return df


def save_to_raw_layer(df: DataFrame, output_path: str):
    """
    Save data to Raw Layer with year/month partition
    No transformation, preserve original data
    """
    print(f"\nSaving to Raw Layer: {output_path}")

    # Add partition columns for storage
    df_with_partition = df \
        .withColumn("year", year("tpep_pickup_datetime")) \
        .withColumn("month", month("tpep_pickup_datetime"))

    # Write to raw layer
    df_with_partition.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(output_path)

    print("Raw Layer saved successfully!")
    print("Partitioned by: year, month")
    print("Raw data preserved without modification")
