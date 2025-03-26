#!/bin/bash

# 检查参数
if [ $# -lt 1 ]; then
    echo "Usage: $0 <date>"
    echo "Example: $0 2024-01-01"
    exit 1
fi

# 设置变量
DATE=$1
DORIS_HOST="your_doris_fe_host"
DORIS_PORT="9030"
DORIS_USER="root"
DORIS_PASSWORD="your_password"
DORIS_DATABASE="retail_analysis"

# 项目根目录
PROJECT_ROOT=$(cd $(dirname $0)/..; pwd)

echo "开始导入数据到Doris，处理日期: $DATE"

# 1. 创建Doris表结构
echo "创建Doris表结构..."
mysql -h ${DORIS_HOST} -P ${DORIS_PORT} -u${DORIS_USER} -p${DORIS_PASSWORD} \
    < ${PROJECT_ROOT}/src/main/resources/doris/create_tables.sql

if [ $? -ne 0 ]; then
    echo "创建Doris表结构失败"
    exit 1
fi

# 2. 提交Spark作业导入数据
echo "提交Spark作业导入数据..."
spark-submit \
    --class com.bigdata.etl.doris.DorisDataLoaderApp \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 4g \
    --executor-memory 4g \
    --executor-cores 2 \
    --num-executors 4 \
    --conf "spark.sql.broadcastTimeout=3600" \
    --conf "spark.sql.shuffle.partitions=200" \
    --jars ${PROJECT_ROOT}/lib/doris-spark-connector.jar \
    ${PROJECT_ROOT}/target/etl-1.0-SNAPSHOT.jar \
    $DATE

if [ $? -ne 0 ]; then
    echo "数据导入失败"
    exit 1
fi

echo "数据导入完成" 