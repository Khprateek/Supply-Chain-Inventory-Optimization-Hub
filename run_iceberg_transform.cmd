@echo off
echo Starting Arch B Batch Transform Compute in Docker...
docker exec spark-master /opt/spark/bin/spark-submit ^
  --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 ^
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.20.18,software.amazon.awssdk:url-connection-client:2.20.18 ^
  /opt/spark/work-dir/streaming/06_iceberg_transform.py
