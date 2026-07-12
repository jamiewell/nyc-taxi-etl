"""
Main entry point for NYC Taxi ETL pipeline
Orchestrates: Source → Raw Layer → Cleaned Layer → Fact Layer → Mart Layer
"""
from pyspark.sql import SparkSession
import sys


def create_spark_session(app_name="NYC Taxi ETL"):
    """Create and configure Spark session"""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def main():
    """
    Main ETL pipeline orchestrator

    Usage:
      spark-submit main.py <input_path> <base_output_path> [zone_lookup_path]

    Example:
      Local:  spark-submit main.py data/sample/yellow_tripdata_2026-01.parquet data/output
      AWS S3: spark-submit main.py s3://bucket/raw/data.parquet s3://bucket/output s3://bucket/ref/taxi_zone_lookup.csv
    """
    if len(sys.argv) < 3:
        print("Usage: spark-submit main.py <input_path> <base_output_path> [zone_lookup_path]")
        print("\nExample:")
        print("  spark-submit main.py data/sample/yellow_tripdata_2026-01.parquet data/output")
        sys.exit(1)

    source_path = sys.argv[1]
    base_output_path = sys.argv[2]
    zone_lookup_path = sys.argv[3] if len(sys.argv) > 3 else "data/reference/taxi_zone_lookup.csv"

    # Define layer paths
    raw_layer_path = f"{base_output_path}/raw/yellow_trip"
    cleaned_layer_path = f"{base_output_path}/cleaned/yellow_trip"
    warehouse_fact_path = f"{base_output_path}/warehouse/fact_taxi_trip"
    dim_vendor_path = f"{base_output_path}/dim/vendor"
    dim_taxi_zone_path = f"{base_output_path}/dim/taxi_zone"
    mart_month_hour_zone_path = f"{base_output_path}/mart/mart_month_hour_zone_trip_metrics"
    mart_month_hour_vendor_path = f"{base_output_path}/mart/mart_month_hour_vendor_trip_metrics"
    mart_month_vendor_cumulative_path = f"{base_output_path}/mart/mart_month_vendor_cumulative_metrics"
    mart_month_zone_cumulative_path = f"{base_output_path}/mart/mart_month_zone_cumulative_metrics"

    print("\n" + "="*70)
    print("NYC Taxi ETL Pipeline - Data Layer Processing")
    print("="*70)
    print(f"Source: {source_path}")
    print(f"Raw Layer: {raw_layer_path}")
    print(f"Cleaned Layer: {cleaned_layer_path}")
    print(f"Fact Layer: {warehouse_fact_path}")
    print(f"Dim Vendor: {dim_vendor_path}")
    print(f"Dim Taxi Zone: {dim_taxi_zone_path}")
    print(f"Zone Lookup Source: {zone_lookup_path}")
    print(f"Mart (month/hour/zone): {mart_month_hour_zone_path}")
    print(f"Mart (month/hour/vendor): {mart_month_hour_vendor_path}")
    print(f"Mart (month/vendor cumulative): {mart_month_vendor_cumulative_path}")
    print(f"Mart (month/zone cumulative): {mart_month_zone_cumulative_path}")
    print("="*70 + "\n")

    # Create Spark session
    spark = create_spark_session()

    try:
        # ===== STEP 1: Save Raw Layer =====
        print("="*70)
        print("STEP 1: Raw Layer - Preserving source data")
        print("="*70)

        from save_raw_layer import read_source_data, save_to_raw_layer

        df_source = read_source_data(spark, source_path)
        save_to_raw_layer(df_source, raw_layer_path)

        print("✓ Raw Layer: COMPLETE\n")

        # ===== STEP 2: Raw → Cleaned Layer =====
        print("="*70)
        print("STEP 2: Cleaned Layer - Quality filtering & standardization")
        print("="*70)

        from raw_to_cleaned import read_raw_data, transform_to_cleaned, write_cleaned_data

        df_raw = read_raw_data(spark, raw_layer_path)
        df_cleaned = transform_to_cleaned(df_raw)
        write_cleaned_data(df_cleaned, cleaned_layer_path)

        print("✓ Cleaned Layer: COMPLETE\n")

        # ===== STEP 3: Cleaned → Fact & Dimension Layer =====
        print("="*70)
        print("STEP 3: Warehouse Layer - Building Fact & Dimension tables")
        print("="*70)

        from cleaned_to_fact import (
            read_cleaned_data,
            build_and_write_dim_vendor,
            build_and_write_dim_taxi_zone,
            transform_to_fact,
            write_fact_table
        )

        # Read cleaned data
        df_cleaned_for_fact = read_cleaned_data(spark, cleaned_layer_path)

        # Build & write dimensions
        build_and_write_dim_vendor(df_cleaned_for_fact, dim_vendor_path)
        build_and_write_dim_taxi_zone(spark, zone_lookup_path, dim_taxi_zone_path)

        # Transform to fact
        df_fact = transform_to_fact(df_cleaned_for_fact)

        # Write fact table
        write_fact_table(df_fact, warehouse_fact_path)

        print("✓ Warehouse Layer: COMPLETE\n")

        # ===== STEP 4: Fact & Dimension → Mart Layer =====
        print("="*70)
        print("STEP 4: Mart Layer - Building mart_month_hour_zone_trip_metrics")
        print("="*70)

        from fact_to_mart_zone import build_and_write_mart_month_hour_zone_trip_metrics

        build_and_write_mart_month_hour_zone_trip_metrics(
            spark, warehouse_fact_path, dim_taxi_zone_path, mart_month_hour_zone_path
        )

        print("✓ Mart Layer (zone): COMPLETE\n")

        # ===== STEP 5: Fact & Dimension → Mart Layer (vendor) =====
        print("="*70)
        print("STEP 5: Mart Layer - Building mart_month_hour_vendor_trip_metrics")
        print("="*70)

        from fact_to_mart_vendor import build_and_write_mart_month_hour_vendor_trip_metrics

        build_and_write_mart_month_hour_vendor_trip_metrics(
            spark, warehouse_fact_path, dim_vendor_path, mart_month_hour_vendor_path
        )

        print("✓ Mart Layer (vendor): COMPLETE\n")

        # ===== STEP 6: Fact & Dimension → Mart Layer (vendor cumulative) =====
        print("="*70)
        print("STEP 6: Mart Layer - Building mart_month_vendor_cumulative_metrics")
        print("="*70)

        from fact_to_mart_vendor_cumulative import build_and_write_mart_month_vendor_cumulative_metrics

        build_and_write_mart_month_vendor_cumulative_metrics(
            spark, warehouse_fact_path, dim_vendor_path, mart_month_vendor_cumulative_path
        )

        print("✓ Mart Layer (vendor cumulative): COMPLETE\n")

        # ===== STEP 7: Fact & Dimension → Mart Layer (zone cumulative) =====
        print("="*70)
        print("STEP 7: Mart Layer - Building mart_month_zone_cumulative_metrics")
        print("="*70)

        from fact_to_mart_zone_cumulative import build_and_write_mart_month_zone_cumulative_metrics

        build_and_write_mart_month_zone_cumulative_metrics(
            spark, warehouse_fact_path, dim_taxi_zone_path, mart_month_zone_cumulative_path
        )

        print("✓ Mart Layer (zone cumulative): COMPLETE\n")

        # ===== Pipeline Summary =====
        print("="*70)
        print("ETL Pipeline Completed Successfully ✓")
        print("="*70)
        print(f"Raw Layer: {raw_layer_path}")
        print(f"Cleaned Layer: {cleaned_layer_path}")
        print(f"Fact Table: {warehouse_fact_path}")
        print(f"Dimension Tables:")
        print(f"  - {dim_vendor_path}")
        print(f"  - {dim_taxi_zone_path}")
        print(f"Mart Tables:")
        print(f"  - {mart_month_hour_zone_path}")
        print(f"  - {mart_month_hour_vendor_path}")
        print(f"  - {mart_month_vendor_cumulative_path}")
        print(f"  - {mart_month_zone_cumulative_path}")
        print("="*70)

    except Exception as e:
        print(f"\n Error in ETL pipeline: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
