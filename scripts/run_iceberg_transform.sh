#!/usr/bin/env bash
# Start continuous Iceberg real-time transform job
set -e

echo "Starting Iceberg Real-Time Transform in Docker..."
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master "local[4]" \
  --driver-memory 1g \
  --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 \
  --conf spark.sql.shuffle.partitions=8 \
  --conf spark.executor.heartbeatInterval=60s \
  --conf spark.network.timeout=300s \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 \
  /opt/spark/work-dir/streaming/06_iceberg_transform.py
