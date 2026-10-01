@echo off
echo Starting Iceberg Streaming Pipeline in Docker...
docker exec spark-master /opt/spark/bin/spark-submit ^
  --conf spark.jars.ivy=/tmp/.ivy2 ^
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 ^
  /opt/spark/work-dir/streaming/04_iceberg_stream.py
