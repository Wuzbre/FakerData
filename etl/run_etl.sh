#!/bin/bash

# 编译项目
mvn clean package

# 获取生成的JAR包路径
JAR_PATH="target/retail-data-etl-1.0-SNAPSHOT.jar"

# 设置Spark配置
SPARK_CONF="--master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  --num-executors 2 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.sql.sources.partitionOverwriteMode=dynamic"

# 运行Spark作业
spark-submit $SPARK_CONF \
  --class com.bigdata.etl.DataExtractor \
  $JAR_PATH 