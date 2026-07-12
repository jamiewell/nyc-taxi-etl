#!/bin/bash

# Submit PySpark job to Docker Spark cluster
# Usage: ./scripts/submit.sh <job_file> <input_path> <output_path> [zone_lookup_path]
# Environment: Set ENV=aws for AWS S3, default is local

set -e

# Get environment (default: local)
ENV=${ENV:-local}

JOB_FILE=${1:-jobs/main.py}

# Set default paths based on environment
if [[ "$ENV" == "aws" ]]; then
    INPUT_PATH=${2:-s3://nyc-taxi-data/raw/yellow_tripdata_2026-01.parquet}
    OUTPUT_PATH=${3:-s3://nyc-taxi-data/processed/}
    ZONE_LOOKUP_PATH=${4:-s3://nyc-taxi-data/ref/taxi_zone_lookup.csv}
    echo "Environment: AWS (S3)"
else
    INPUT_PATH=${2:-data/sample/yellow_tripdata_2026-01.parquet}
    OUTPUT_PATH=${3:-data/output/processed}
    ZONE_LOOKUP_PATH=${4:-data/reference/taxi_zone_lookup.csv}
    echo "Environment: Local"
fi

echo "========================================"
echo "Submitting Spark Job to Docker Cluster"
echo "========================================"
echo "Job File: $JOB_FILE"
echo "Input Path: $INPUT_PATH"
echo "Output Path: $OUTPUT_PATH"
echo "Zone Lookup Path: $ZONE_LOOKUP_PATH"
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
  $CONTAINER_ZONE_LOOKUP_PATH

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
