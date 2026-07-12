#!/bin/bash

# Start Spark Master
echo "Starting Spark Master..."
$SPARK_HOME/sbin/start-master.sh

# Wait for master to be ready
echo "Waiting for Spark Master to be ready..."
sleep 10

# Verify master is listening on port 7077
for i in {1..30}; do
    if nc -z localhost 7077 2>/dev/null; then
        echo "Master is ready!"
        break
    fi
    echo "Waiting for master... ($i/30)"
    sleep 2
done

# Start Spark Worker (connect to local master)
echo "Starting Spark Worker..."
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077

# Start History Server
echo "Starting Spark History Server..."
$SPARK_HOME/sbin/start-history-server.sh

echo "Spark Cluster Started!"
echo "Master Web UI: http://localhost:8080"
echo "Worker Web UI: http://localhost:8081"
echo "History Server: http://localhost:18080"
echo "Application UI will be available at: http://localhost:4040 (when job is running)"

# Keep container running
tail -f $SPARK_HOME/logs/*
