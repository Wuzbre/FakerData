#!/bin/bash

# 销售数据仓库ETL流程执行脚本
# 负责调度整个数据处理流程，包括从MySQL到ODS、ODS到DWD、DWD到DWS的转换以及数据质量检查

# 设置环境变量
export HADOOP_HOME=/opt/module/hadoop
export HIVE_HOME=/opt/module/hive
export DATAX_HOME=/opt/module/datax
export PATH=$PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin:$DATAX_HOME/bin

# 项目根目录
PROJECT_HOME=$(cd $(dirname $0)/..; pwd)
cd $PROJECT_HOME

# 日志目录
LOG_DIR=$PROJECT_HOME/logs
mkdir -p $LOG_DIR

# 获取当前日期，默认是昨天的数据
if [ -z "$1" ]; then
    dt=$(date -d "yesterday" +%Y%m%d)
else
    dt=$1
fi

# 日志文件
log_file=$LOG_DIR/etl_pipeline_$dt.log

# 记录日志函数
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a $log_file
}

# 检查上一步是否成功
check_success() {
    if [ $? -ne 0 ]; then
        log "ERROR: $1 失败，退出流程"
        exit 1
    else
        log "INFO: $1 成功"
    fi
}

log "开始销售数据仓库ETL流程，处理日期：$dt"

# 步骤1：MySQL到ODS层数据抽取
log "步骤1：开始MySQL到ODS层数据抽取"

# 运行DataX作业进行数据抽取
cd $PROJECT_HOME/datax
python $PROJECT_HOME/datax/run_datax_jobs.py --date $dt
check_success "MySQL到ODS层数据抽取"

# 添加ODS层分区
log "为ODS层表添加分区"
hive -hivevar dt=$dt -f $PROJECT_HOME/hive/add_ods_partitions.hql
check_success "添加ODS层分区"

# 步骤2：ODS到DWD层数据转换
log "步骤2：开始ODS到DWD层数据转换"
hive -hivevar dt=$dt -f $PROJECT_HOME/hive/ods_to_dwd.hql
check_success "ODS到DWD层数据转换"

# 步骤3：DWD到DWS层数据汇总
log "步骤3：开始DWD到DWS层数据汇总"
hive -hivevar dt=$dt -f $PROJECT_HOME/hive/dwd_to_dws.hql
check_success "DWD到DWS层数据汇总"

# 步骤4：数据质量检查
log "步骤4：开始数据质量检查"
hive -hivevar dt=$dt -f $PROJECT_HOME/hive/data_quality_check.hql > $LOG_DIR/data_quality_$dt.txt
check_success "数据质量检查"

# 检查数据质量检查结果
severe_issues=$(grep -c "HIGH" $LOG_DIR/data_quality_$dt.txt)
if [ $severe_issues -gt 0 ]; then
    log "警告：数据质量检查发现 $severe_issues 个严重问题，请查看 $LOG_DIR/data_quality_$dt.txt"
fi

# 步骤5：Hive数据同步到Doris（如果配置了）
if [ -f "$PROJECT_HOME/doris/hive_to_doris_sync.py" ]; then
    log "步骤5：开始Hive数据同步到Doris"
    python $PROJECT_HOME/doris/hive_to_doris_sync.py --date $dt
    check_success "Hive数据同步到Doris"
fi

# 步骤6：生成数据加载报告
log "步骤6：生成数据加载报告"

# 获取各层数据量
ods_count=$(hive -S -e "SELECT COUNT(*) FROM purchase_sales_ods.ods_sales_orders WHERE dt='$dt'")
dwd_count=$(hive -S -e "SELECT COUNT(*) FROM purchase_sales_dwd.dwd_fact_sales_order WHERE dt='$dt'")
dws_day_count=$(hive -S -e "SELECT COUNT(*) FROM purchase_sales_dws.dws_sales_day_agg WHERE dt='$dt'")

# 生成简单的报告
cat > $LOG_DIR/etl_report_$dt.txt << EOF
销售数据仓库ETL报告
===================
处理日期: $dt
处理时间: $(date '+%Y-%m-%d %H:%M:%S')

数据加载统计:
- ODS层销售订单数: $ods_count
- DWD层销售订单数: $dwd_count
- DWS层销售日汇总记录数: $dws_day_count

数据质量:
- 严重问题数: $severe_issues
- 详细结果请查看: $LOG_DIR/data_quality_$dt.txt

流程执行日志:
- 详细日志请查看: $log_file
EOF

log "ETL流程报告已生成: $LOG_DIR/etl_report_$dt.txt"
log "销售数据仓库ETL流程执行完成"

exit 0 