#!/bin/bash

# Submit PySpark job to Docker Spark cluster
# Usage: ./scripts/submit.sh <job_file> <input_path> <output_path> [zone_lookup_path] [taxi_types] [start_year_month] [end_year_month] [overwrite_mode]
# Environment: Set ENV=aws for AWS S3, default is local
# taxi_types: comma-separated (yellow,green,fhvhv,fhv) or "all". Default: yellow
#             Can also be set via TAXI_TYPES env var.
# start_year_month / end_year_month: format YYYY-MM, inclusive range. Optional -
#             omit to process all available months. Can also be set via
#             START_YEAR_MONTH / END_YEAR_MONTH env vars. When set, input_path
#             must be the collector bucket base prefix (.../raw/nyc_taxi), not
#             a single file.
# overwrite_mode: "dynamic" (default, safe for incremental range runs - only
#             touches the year=/month= partitions being written) or "static"
#             (Spark's own default - wipes the ENTIRE output path every run;
#             only use for a deliberate full rebuild). Can also be set via
#             OVERWRITE_MODE env var. See docs/known_issues_and_fixes.md #5.

set -e

# Get environment (default: local)
ENV=${ENV:-local}

JOB_FILE=${1:-jobs/main.py}

# Set default paths based on environment
if [[ "$ENV" == "aws" ]]; then
    INPUT_PATH=${2:-s3://nyc-taxi-collector-raw/raw/nyc_taxi/yellow}
    OUTPUT_PATH=${3:-s3://nyc-taxi-data/processed/}
    ZONE_LOOKUP_PATH=${4:-s3://nyc-taxi-data/ref/taxi_zone_lookup.csv}
    echo "Environment: AWS (S3)"
else
    INPUT_PATH=${2:-data/sample/yellow_tripdata_2026-01.parquet}
    OUTPUT_PATH=${3:-data/output/processed}
    ZONE_LOOKUP_PATH=${4:-data/reference/taxi_zone_lookup.csv}
    echo "Environment: Local"
fi

TAXI_TYPES=${5:-${TAXI_TYPES:-yellow}}
START_YEAR_MONTH=${6:-${START_YEAR_MONTH:-}}
END_YEAR_MONTH=${7:-${END_YEAR_MONTH:-}}
OVERWRITE_MODE=${8:-${OVERWRITE_MODE:-dynamic}}

echo "========================================"
echo "Submitting Spark Job to Docker Cluster"
echo "========================================"
echo "Job File: $JOB_FILE"
echo "Input Path: $INPUT_PATH"
echo "Output Path: $OUTPUT_PATH"
echo "Zone Lookup Path: $ZONE_LOOKUP_PATH"
echo "Taxi Types: $TAXI_TYPES"
echo "Collection Range: ${START_YEAR_MONTH:-earliest} ~ ${END_YEAR_MONTH:-latest}"
echo "Overwrite Mode: $OVERWRITE_MODE"
echo "========================================"

# Determine if paths need container prefix
container_path() {
    local path="$1"
    if [[ "$path" =~ ^s3:// ]] || [[ "$path" =~ ^s3a:// ]] || [[ "$path" =~ ^gs:// ]] || [[ "$path" =~ ^hdfs:// ]]; then
        echo "$path"
    else
        echo "/opt/spark/$path"
    fi
}

CONTAINER_INPUT_PATH=$(container_path "$INPUT_PATH")
CONTAINER_OUTPUT_PATH=$(container_path "$OUTPUT_PATH")
CONTAINER_ZONE_LOOKUP_PATH=$(container_path "$ZONE_LOOKUP_PATH")
CONTAINER_JOB_FILE="/opt/spark/$JOB_FILE"

EXTRA_ARGS=(--taxi-types "$TAXI_TYPES" --overwrite-mode "$OVERWRITE_MODE")
if [[ -n "$START_YEAR_MONTH" ]]; then
    EXTRA_ARGS+=(--start-year-month "$START_YEAR_MONTH")
fi
if [[ -n "$END_YEAR_MONTH" ]]; then
    EXTRA_ARGS+=(--end-year-month "$END_YEAR_MONTH")
fi

# Submit job to Spark cluster running in Docker
docker exec nyc-taxi-spark /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=/tmp/spark-events \
  $CONTAINER_JOB_FILE \
  $CONTAINER_INPUT_PATH \
  $CONTAINER_OUTPUT_PATH \
  $CONTAINER_ZONE_LOOKUP_PATH \
  "${EXTRA_ARGS[@]}"

echo ""
echo "========================================"
echo "Job Submission Complete!"
echo "========================================"
echo "Check Web UIs:"
echo "  - Master UI: http://localhost:8080"
echo "  - Worker UI: http://localhost:8081"
echo "  - Application UI: http://localhost:4040 (while running)"
echo "  - History Server: http://localhost:18080 (after completion)"
echo "========================================"
