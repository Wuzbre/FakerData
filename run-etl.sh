#!/bin/bash

# 销售数据ETL执行脚本
# 功能: 执行Spark SQL ETL流程，包括:
#   1. MySQL到ODS层的数据抽取
#   2. ODS到DWD层的数据转换
#   3. DWD到DWS层的数据汇总
#   4. 数据质量检查
#   5. Hive到Doris的数据同步

# 设置日期参数
if [ -z "$1" ]; then
  PROCESS_DATE=$(date -d "yesterday" +%Y%m%d)
else
  PROCESS_DATE=$1
fi

# 设置Spark参数
SPARK_MASTER=${SPARK_MASTER:-"local[*]"}
DRIVER_MEMORY=${DRIVER_MEMORY:-"2g"}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-"2g"}
EXECUTOR_CORES=${EXECUTOR_CORES:-"2"}

# 日志目录
LOG_DIR="log"
mkdir -p ${LOG_DIR}
LOG_FILE="${LOG_DIR}/etl_${PROCESS_DATE}.log"

# JAR包路径
JAR_PATH="target/sales-data-warehouse-1.0-SNAPSHOT-jar-with-dependencies.jar"

# 定义彩色输出函数
info() {
  echo -e "\033[34m[INFO]\033[0m $1"
}

warning() {
  echo -e "\033[33m[WARN]\033[0m $1"
}

error() {
  echo -e "\033[31m[ERROR]\033[0m $1"
}

success() {
  echo -e "\033[32m[SUCCESS]\033[0m $1"
}

# 检查JAR包是否存在
if [ ! -f "${JAR_PATH}" ]; then
  error "找不到JAR包: ${JAR_PATH}，请先运行: mvn clean package"
  exit 1
fi

# 打印执行信息
info "开始执行销售数据ETL流程"
info "处理日期: ${PROCESS_DATE}"
info "日志文件: ${LOG_FILE}"

# 执行Spark作业
info "提交Spark作业..."
spark-submit \
  --master ${SPARK_MASTER} \
  --driver-memory ${DRIVER_MEMORY} \
  --executor-memory ${EXECUTOR_MEMORY} \
  --executor-cores ${EXECUTOR_CORES} \
  --conf "spark.dynamicAllocation.enabled=false" \
  --conf "spark.sql.shuffle.partitions=50" \
  --conf "spark.sql.warehouse.dir=/user/hive/warehouse" \
  --class com.sales.SalesDataETL \
  ${JAR_PATH} \
  --date ${PROCESS_DATE} \
  --all 2>&1 | tee ${LOG_FILE}

# 检查执行结果
if [ $? -eq 0 ]; then
  success "销售数据ETL流程执行成功！"
  exit 0
else
  error "销售数据ETL流程执行失败，请查看日志: ${LOG_FILE}"
  exit 1
fi 