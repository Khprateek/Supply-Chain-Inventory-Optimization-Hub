@echo off
echo Starting Architecture B Iceberg Real-Time Transform in Docker...
docker exec spark-master /opt/spark/bin/spark-submit ^
  --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 ^
  --conf spark.executor.heartbeatInterval=60s ^
  --conf spark.network.timeout=300s ^
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 ^
  /opt/spark/work-dir/streaming/06_iceberg_transform.py
