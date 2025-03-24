#!/bin/bash

# 销售数据ETL执行脚本
# 功能: 执行Spark SQL ETL流程，包括:
#   1. 清理过期分区
#   2. MySQL到ODS层的数据抽取
#   3. ODS到DWD层的数据转换
#   4. DWD到DWS层的数据汇总
#   5. 数据质量检查
#   6. Hive到Doris的数据同步

# 设置日期参数和重试次数
if [ -z "$1" ]; then
  PROCESS_DATE=$(date -d "yesterday" +%Y%m%d)
else
  PROCESS_DATE=$1
fi
MAX_RETRIES=3

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
  echo -e "\033[34m[INFO]\033[0m $1" | tee -a ${LOG_FILE}
}

warning() {
  echo -e "\033[33m[WARN]\033[0m $1" | tee -a ${LOG_FILE}
}

error() {
  echo -e "\033[31m[ERROR]\033[0m $1" | tee -a ${LOG_FILE}
}

success() {
  echo -e "\033[32m[SUCCESS]\033[0m $1" | tee -a ${LOG_FILE}
}

# 定义运行Spark作业的函数
run_spark_job() {
  local stage=$1
  local stage_arg=$2
  local retry_count=0
  
  while [ $retry_count -lt $MAX_RETRIES ]; do
    info "开始执行阶段: $stage (尝试 $((retry_count+1))/$MAX_RETRIES)"
    
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
      ${stage_arg} 2>&1 | tee -a ${LOG_FILE}
    
    if [ $? -eq 0 ]; then
      success "$stage 执行成功！"
      return 0
    else
      error "$stage 执行失败，尝试重试..."
      retry_count=$((retry_count+1))
      sleep 60  # 等待1分钟后重试
    fi
  done
  
  error "$stage 在 $MAX_RETRIES 次尝试后仍然失败！"
  return 1
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

# 按顺序执行各阶段流程
if [ -z "$2" ]; then
  # 没有指定阶段，按顺序执行所有阶段
  stages=(
    "清理过期分区:--clean-expired"
    "MySQL到ODS抽取:--mysql-to-ods"
    "ODS到DWD转换:--ods-to-dwd"
    "DWD到DWS汇总:--dwd-to-dws"
    "数据质量检查:--dq-check"
    "Hive到Doris同步:--sync-to-doris"
  )
  
  for stage_info in "${stages[@]}"; do
    stage_name=${stage_info%%:*}
    stage_arg=${stage_info#*:}
    
    run_spark_job "$stage_name" "$stage_arg"
    if [ $? -ne 0 ]; then
      error "ETL流程在阶段 '$stage_name' 失败，终止执行！"
      exit 1
    fi
  done
  
  success "销售数据ETL流程全部成功完成！"
else
  # 指定了阶段，只执行特定阶段
  case "$2" in
    --clean-expired)
      run_spark_job "清理过期分区" "--clean-expired"
      ;;
    --mysql-to-ods)
      run_spark_job "MySQL到ODS抽取" "--mysql-to-ods"
      ;;
    --ods-to-dwd)
      run_spark_job "ODS到DWD转换" "--ods-to-dwd"
      ;;
    --dwd-to-dws)
      run_spark_job "DWD到DWS汇总" "--dwd-to-dws"
      ;;
    --dq-check)
      run_spark_job "数据质量检查" "--dq-check"
      ;;
    --sync-to-doris)
      run_spark_job "Hive到Doris同步" "--sync-to-doris"
      ;;
    *)
      error "未知的阶段参数: $2"
      exit 1
      ;;
  esac
fi

exit 0