@echo off
echo Running Architecture B Iceberg Batch Transformations in Docker...
docker exec spark-master /opt/spark/bin/spark-submit ^
  --conf spark.jars.ivy=/opt/spark/work-dir/.ivy2 ^
  --driver-java-options "-XX:TieredStopAtLevel=1" ^
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.iceberg:iceberg-aws-bundle:1.5.0 ^
  /opt/spark/work-dir/complex_transforms/05_iceberg_transforms.py
