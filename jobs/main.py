"""
Main entry point for NYC Taxi ETL pipeline
Orchestrates: Source → Raw Layer → Cleaned Layer → Fact Layer → Mart Layer
"""
import argparse
from pyspark.sql import SparkSession

VALID_TAXI_TYPES = ("yellow", "green", "fhvhv", "fhv")


VALID_OVERWRITE_MODES = ("static", "dynamic")


def create_spark_session(app_name="NYC Taxi ETL", overwrite_mode="dynamic"):
    """
    Create and configure Spark session.

    partitionOverwriteMode controls what "overwrite" mode does on a
    partitioned write (see --overwrite-mode help / docs/known_issues_and_fixes.md #5):
      - dynamic (default, safe for --start-year-month/--end-year-month):
        only the year=/month= partitions present in the DataFrame being
        written are replaced; other months already on disk are untouched.
      - static (Spark's own default): the ENTIRE output path is deleted and
        rewritten from the DataFrame's contents. Re-running a narrow date
        range in this mode deletes every other month previously written to
        that path - only use it when you deliberately want a full rebuild.
    """
    if overwrite_mode not in VALID_OVERWRITE_MODES:
        raise ValueError(
            f"Invalid overwrite_mode: {overwrite_mode!r}. Must be one of {VALID_OVERWRITE_MODES}"
        )

    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.sources.partitionOverwriteMode", overwrite_mode) \
        .config("spark.sql.optimizer.plannedWrite.enabled", "false") \
        .getOrCreate()


def parse_taxi_types(raw_value: str):
    if raw_value.strip().lower() == "all":
        return list(VALID_TAXI_TYPES)

    taxi_types = [t.strip() for t in raw_value.split(",") if t.strip()]
    invalid = [t for t in taxi_types if t not in VALID_TAXI_TYPES]
    if invalid:
        raise ValueError(
            f"Invalid taxi type(s): {invalid}. Must be one of {VALID_TAXI_TYPES} or 'all'"
        )
    if not taxi_types:
        raise ValueError("--taxi-types must not be empty")
    return taxi_types


def parse_args():
    parser = argparse.ArgumentParser(
        description="NYC Taxi ETL pipeline: Source -> Raw -> Cleaned -> Fact/Dim -> Mart"
    )
    parser.add_argument(
        "source_path",
        help=(
            "Input path. When --taxi-types selects exactly one type, used as-is "
            "(a single file/prefix). When multiple types are selected, treated as "
            "a base prefix and '<source_path>/<taxi_type>' is read per type "
            "(matches the collector bucket layout raw/nyc_taxi/<taxi_type>/...)."
        ),
    )
    parser.add_argument("base_output_path", help="Base output path for all layers")
    parser.add_argument(
        "zone_lookup_path",
        nargs="?",
        default="data/reference/taxi_zone_lookup.csv",
        help="Path to taxi_zone_lookup.csv for dim_taxi_zone (default: data/reference/taxi_zone_lookup.csv)",
    )
    parser.add_argument(
        "--taxi-types",
        default="yellow",
        help=(
            "Comma-separated taxi types to process (yellow,green,fhvhv,fhv) or "
            "'all' for every type. Default: yellow"
        ),
    )
    parser.add_argument(
        "--start-year-month",
        default=None,
        help=(
            "Inclusive start of the collection range, format YYYY-MM. When set "
            "(alone or with --end-year-month), source_path is always treated as "
            "the collector bucket base prefix ('<source_path>/<taxi_type>/"
            "year=Y/month=M') regardless of how many taxi types are selected, "
            "and only the months that actually exist there are processed - "
            "there is no metadata table, so existence is checked against the "
            "source itself. Default: earliest available."
        ),
    )
    parser.add_argument(
        "--end-year-month",
        default=None,
        help="Inclusive end of the collection range, format YYYY-MM. Default: latest available.",
    )
    parser.add_argument(
        "--overwrite-mode",
        default="dynamic",
        choices=VALID_OVERWRITE_MODES,
        help=(
            "Spark partition overwrite behavior for raw/cleaned writes. "
            "'dynamic' (default) only replaces the year=/month= partitions being "
            "written, leaving other months on disk untouched - safe to use with "
            "--start-year-month/--end-year-month for incremental runs. "
            "'static' deletes and rewrites the ENTIRE output path every run "
            "(Spark's own default) - only use for a deliberate full rebuild, "
            "since combined with a narrow date range it will delete previously "
            "processed months. See docs/known_issues_and_fixes.md #5."
        ),
    )
    return parser.parse_args()


def run_raw_and_cleaned_layer(spark, source_path, base_output_path, taxi_type, is_multi_type,
                               start_year_month=None, end_year_month=None):
    """
    Runs Raw Layer + Cleaned Layer for a single taxi type.
    Returns the cleaned layer path so the caller can feed it into Fact/Dim.
    """
    from save_raw_layer import read_source_data, save_to_raw_layer, save_month_to_raw_layer
    from raw_to_cleaned import read_raw_data, transform_to_cleaned, write_cleaned_data

    raw_layer_path = f"{base_output_path}/raw/{taxi_type}_trip"
    cleaned_layer_path = f"{base_output_path}/cleaned/{taxi_type}_trip"

    if start_year_month or end_year_month:
        from collection_range import resolve_year_months, raw_layer_month_paths
        year_months = resolve_year_months(spark, source_path, taxi_type, start_year_month, end_year_month)
        type_path = f"{source_path.rstrip('/')}/{taxi_type}"

        # Months are read and written ONE AT A TIME here, not merged into a
        # single spark.read.parquet(*paths) call. TLC's source files aren't
        # internally consistent about physical Parquet encoding for the same
        # logical column across different months (e.g. congestion_surcharge
        # as INT32 in one month's file, DOUBLE in another's) - and Spark's
        # Parquet reader cannot reconcile that within one combined read, with
        # or without the vectorized reader (confirmed: vectorized raises
        # SchemaColumnConvertNotSupportedException, non-vectorized raises a
        # ClassCastException instead - same underlying limitation, no config
        # fixes it). Reading month-by-month means each individual read only
        # ever touches one file's schema, so the mismatch never has to be
        # reconciled at read time. See docs/known_issues_and_fixes.md.
        print("=" * 70)
        print(f"STEP 1: Raw Layer ({taxi_type}) - Preserving source data")
        print("=" * 70)
        print(f"Raw Layer: {raw_layer_path}")
        for y, m in year_months:
            month_source_path = f"{type_path}/year={y}/month={m:02d}"
            print(f"  Source ({y}-{m:02d}): {month_source_path}")
            df_source = read_source_data(spark, month_source_path)
            # save_month_to_raw_layer (not save_to_raw_layer) - writes to an
            # explicit year=/month= path keyed by this source file's nominal
            # month, so a later month's stray boundary rows can never
            # overwrite an earlier month's partition. See
            # jobs/save_raw_layer.py and docs/known_issues_and_fixes.md.
            save_month_to_raw_layer(df_source, raw_layer_path, taxi_type, y, m)
        print(f"✓ Raw Layer ({taxi_type}): COMPLETE\n")

        print("=" * 70)
        print(f"STEP 2: Cleaned Layer ({taxi_type}) - Quality filtering & standardization")
        print("=" * 70)
        print(f"Cleaned Layer: {cleaned_layer_path}")
        cleaned_dfs = []
        for month_raw_path in raw_layer_month_paths(raw_layer_path, year_months):
            df_raw_month = read_raw_data(spark, month_raw_path)
            # transform_to_cleaned casts fee/distance columns to a stable
            # DoubleType (see raw_to_cleaned._round_double), so by the time
            # these per-month DataFrames are unioned below, the physical
            # type mismatch from the source is already resolved.
            cleaned_dfs.append(transform_to_cleaned(df_raw_month, taxi_type=taxi_type))
        df_cleaned = cleaned_dfs[0]
        for other in cleaned_dfs[1:]:
            df_cleaned = df_cleaned.unionByName(other)
        write_cleaned_data(df_cleaned, cleaned_layer_path)
        print(f"✓ Cleaned Layer ({taxi_type}): COMPLETE\n")
    else:
        type_source_path = f"{source_path}/{taxi_type}" if is_multi_type else source_path

        print("=" * 70)
        print(f"STEP 1: Raw Layer ({taxi_type}) - Preserving source data")
        print("=" * 70)
        print(f"Source: {type_source_path}")
        print(f"Raw Layer: {raw_layer_path}")

        df_source = read_source_data(spark, type_source_path)
        save_to_raw_layer(df_source, raw_layer_path, taxi_type=taxi_type)

        print(f"✓ Raw Layer ({taxi_type}): COMPLETE\n")

        print("=" * 70)
        print(f"STEP 2: Cleaned Layer ({taxi_type}) - Quality filtering & standardization")
        print("=" * 70)
        print(f"Cleaned Layer: {cleaned_layer_path}")

        df_raw = read_raw_data(spark, raw_layer_path)
        df_cleaned = transform_to_cleaned(df_raw, taxi_type=taxi_type)
        write_cleaned_data(df_cleaned, cleaned_layer_path)

        print(f"✓ Cleaned Layer ({taxi_type}): COMPLETE\n")

    return cleaned_layer_path


def run_fact_dim_mart_layers(spark, cleaned_layer_path, base_output_path, zone_lookup_path):
    """
    Fact/Dimension/Mart layers currently assume the yellow taxi schema
    (VendorID, fare_amount, passenger_count, ...) produced by
    raw_to_cleaned._transform_yellow. Wiring green/fhvhv/fhv into these
    layers is separate follow-up work.
    """
    warehouse_fact_path = f"{base_output_path}/warehouse/fact_taxi_trip"
    dim_vendor_path = f"{base_output_path}/dim/vendor"
    dim_taxi_zone_path = f"{base_output_path}/dim/taxi_zone"
    mart_month_hour_zone_path = f"{base_output_path}/mart/mart_month_hour_zone_trip_metrics"
    mart_month_hour_vendor_path = f"{base_output_path}/mart/mart_month_hour_vendor_trip_metrics"
    mart_month_vendor_cumulative_path = f"{base_output_path}/mart/mart_month_vendor_cumulative_metrics"
    mart_month_zone_cumulative_path = f"{base_output_path}/mart/mart_month_zone_cumulative_metrics"

    print(f"Fact Layer: {warehouse_fact_path}")
    print(f"Dim Vendor: {dim_vendor_path}")
    print(f"Dim Taxi Zone: {dim_taxi_zone_path}")
    print(f"Zone Lookup Source: {zone_lookup_path}")
    print(f"Mart (month/hour/zone): {mart_month_hour_zone_path}")
    print(f"Mart (month/hour/vendor): {mart_month_hour_vendor_path}")
    print(f"Mart (month/vendor cumulative): {mart_month_vendor_cumulative_path}")
    print(f"Mart (month/zone cumulative): {mart_month_zone_cumulative_path}")
    print("=" * 70 + "\n")

    # ===== STEP 3: Cleaned → Fact & Dimension Layer =====
    print("=" * 70)
    print("STEP 3: Warehouse Layer - Building Fact & Dimension tables")
    print("=" * 70)

    from cleaned_to_fact import (
        read_cleaned_data,
        build_and_write_dim_vendor,
        build_and_write_dim_taxi_zone,
        transform_to_fact,
        write_fact_table
    )

    df_cleaned_for_fact = read_cleaned_data(spark, cleaned_layer_path)

    build_and_write_dim_vendor(df_cleaned_for_fact, dim_vendor_path)
    build_and_write_dim_taxi_zone(spark, zone_lookup_path, dim_taxi_zone_path)

    df_fact = transform_to_fact(df_cleaned_for_fact)
    write_fact_table(df_fact, warehouse_fact_path)

    print("✓ Warehouse Layer: COMPLETE\n")

    # ===== STEP 4-7: Fact & Dimension → Mart Layer =====
    print("=" * 70)
    print("STEP 4: Mart Layer - Building mart_month_hour_zone_trip_metrics")
    print("=" * 70)
    from fact_to_mart_zone import build_and_write_mart_month_hour_zone_trip_metrics
    build_and_write_mart_month_hour_zone_trip_metrics(
        spark, warehouse_fact_path, dim_taxi_zone_path, mart_month_hour_zone_path
    )
    print("✓ Mart Layer (zone): COMPLETE\n")

    print("=" * 70)
    print("STEP 5: Mart Layer - Building mart_month_hour_vendor_trip_metrics")
    print("=" * 70)
    from fact_to_mart_vendor import build_and_write_mart_month_hour_vendor_trip_metrics
    build_and_write_mart_month_hour_vendor_trip_metrics(
        spark, warehouse_fact_path, dim_vendor_path, mart_month_hour_vendor_path
    )
    print("✓ Mart Layer (vendor): COMPLETE\n")

    print("=" * 70)
    print("STEP 6: Mart Layer - Building mart_month_vendor_cumulative_metrics")
    print("=" * 70)
    from fact_to_mart_vendor_cumulative import build_and_write_mart_month_vendor_cumulative_metrics
    build_and_write_mart_month_vendor_cumulative_metrics(
        spark, warehouse_fact_path, dim_vendor_path, mart_month_vendor_cumulative_path
    )
    print("✓ Mart Layer (vendor cumulative): COMPLETE\n")

    print("=" * 70)
    print("STEP 7: Mart Layer - Building mart_month_zone_cumulative_metrics")
    print("=" * 70)
    from fact_to_mart_zone_cumulative import build_and_write_mart_month_zone_cumulative_metrics
    build_and_write_mart_month_zone_cumulative_metrics(
        spark, warehouse_fact_path, dim_taxi_zone_path, mart_month_zone_cumulative_path
    )
    print("✓ Mart Layer (zone cumulative): COMPLETE\n")

    print("=" * 70)
    print("ETL Pipeline Completed Successfully ✓")
    print("=" * 70)
    print(f"Fact Table: {warehouse_fact_path}")
    print("Dimension Tables:")
    print(f"  - {dim_vendor_path}")
    print(f"  - {dim_taxi_zone_path}")
    print("Mart Tables:")
    print(f"  - {mart_month_hour_zone_path}")
    print(f"  - {mart_month_hour_vendor_path}")
    print(f"  - {mart_month_vendor_cumulative_path}")
    print(f"  - {mart_month_zone_cumulative_path}")
    print("=" * 70)


def main():
    """
    Main ETL pipeline orchestrator

    Usage:
      spark-submit main.py <source_path> <base_output_path> [zone_lookup_path] [--taxi-types TYPES]

    Examples:
      Single type (default, backward compatible):
        spark-submit main.py data/sample/yellow_tripdata_2026-01.parquet data/output

      Multiple types from the collector bucket layout
      (source_path is treated as base prefix, "<source_path>/<taxi_type>" is read per type):
        spark-submit main.py s3://nyc-taxi-collector-raw/raw/nyc_taxi s3://bucket/output \\
          data/reference/taxi_zone_lookup.csv --taxi-types yellow,green

      All four types:
        spark-submit main.py s3://nyc-taxi-collector-raw/raw/nyc_taxi s3://bucket/output --taxi-types all

      Specific collection range (only months that actually exist in the source are processed):
        spark-submit main.py s3://nyc-taxi-collector-raw/raw/nyc_taxi s3://bucket/output \\
          --taxi-types yellow,green --start-year-month 2025-06 --end-year-month 2025-12

      Force a full rebuild of the output path instead of incremental partition updates:
        spark-submit main.py s3://nyc-taxi-collector-raw/raw/nyc_taxi s3://bucket/output \\
          --taxi-types all --overwrite-mode static
    """
    args = parse_args()
    taxi_types = parse_taxi_types(args.taxi_types)
    is_multi_type = len(taxi_types) > 1
    has_range = bool(args.start_year_month or args.end_year_month)

    print("\n" + "=" * 70)
    print("NYC Taxi ETL Pipeline - Data Layer Processing")
    print("=" * 70)
    print(f"Taxi types: {taxi_types}")
    print(f"Source: {args.source_path}")
    print(f"Base output: {args.base_output_path}")
    print(f"Overwrite mode: {args.overwrite_mode}")
    if has_range:
        print(f"Collection range: {args.start_year_month or 'earliest'} ~ {args.end_year_month or 'latest'}")

    if args.overwrite_mode == "static" and has_range:
        print(
            "WARNING: --overwrite-mode static with a narrowed collection range will "
            "DELETE any previously written months outside this run's range under "
            "each output path (raw/cleaned). See docs/known_issues_and_fixes.md #5."
        )

    spark = create_spark_session(overwrite_mode=args.overwrite_mode)

    try:
        cleaned_paths_by_type = {}
        for taxi_type in taxi_types:
            cleaned_paths_by_type[taxi_type] = run_raw_and_cleaned_layer(
                spark, args.source_path, args.base_output_path, taxi_type, is_multi_type,
                start_year_month=args.start_year_month, end_year_month=args.end_year_month
            )

        # Fact/Dim/Mart currently only understand the yellow schema.
        if "yellow" in cleaned_paths_by_type:
            run_fact_dim_mart_layers(
                spark, cleaned_paths_by_type["yellow"], args.base_output_path, args.zone_lookup_path
            )
        else:
            print("=" * 70)
            print(
                "Skipping Fact/Dimension/Mart layers: 'yellow' was not in --taxi-types. "
                "Fact/Mart jobs currently only support the yellow schema."
            )
            print(f"Cleaned layers written for: {list(cleaned_paths_by_type)}")
            print("=" * 70)

    except Exception as e:
        print(f"\n Error in ETL pipeline: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
