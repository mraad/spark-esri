#!/usr/bin/env bash

export SPARK_LOCAL_IP=$(hostname -I | awk '{print $1}')

export PYSPARK_DRIVER_PYTHON=jupyter
export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0 --port 8888 --allow-root --no-browser --NotebookApp.token=""'

export RAPIDS_DIR="/data"

export JARS="${JARS:+$JARS,}${RAPIDS_DIR}/cudf-0.14-cuda10-2.jar"
export JARS="${JARS:+$JARS,}${RAPIDS_DIR}/rapids-4-spark_2.12-0.1.0.jar"

$SPARK_HOME/bin/pyspark \
  --master local[*] \
  --jars "${JARS}" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.driver.memory=16g \
  --conf spark.executor.instances=1 \
  --conf spark.executor.cores=8 \
  --conf spark.rapids.sql.concurrentGpuTasks=1 \
  --conf spark.rapids.memory.pinnedPool.size=4G \
  --conf spark.locality.wait=0s \
  --conf spark.sql.files.maxPartitionBytes=512m \
  --conf spark.sql.shuffle.partitions=16 \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.kryo.registrator=com.nvidia.spark.rapids.GpuKryoRegistrator \
  --conf spark.sql.execution.arrow.pyspark.enabled=true
