package com.sales.quality

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Logger
import com.sales.utils.{ConfigUtils, DateUtils}
import scala.util.{Try, Success, Failure}

/**
 * 数据质量检查类
 * 负责进行各种数据质量检查，包括完整性、一致性和准确性等方面的检查
 */
object DataQualityCheck {
  private val logger = Logger.getLogger(this.getClass)
  
  /**
   * 执行数据质量检查
   * @param spark SparkSession实例
   * @param processDate 处理日期，格式：yyyyMMdd
   * @return 检查是否通过
   */
  def process(spark: SparkSession, processDate: String): Boolean = {
    logger.info(s"开始执行数据质量检查, 处理日期: $processDate")
    
    try {
      import spark.implicits._
      
      // 确保数据质量检查结果数据库存在
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${ConfigUtils.DQ_DATABASE}")
      spark.sql(s"USE ${ConfigUtils.DQ_DATABASE}")
      
      // 创建检查结果表
      createCheckResultsTable(spark)
      
      // 按照类别执行各项检查
      val odsCheckSuccess = Try {
        // ODS层空值检查
        checkOdsNullValues(spark, processDate)
        
        // ODS层记录数检查
        checkOdsRecordCount(spark, processDate)
        
        // ODS层主键唯一性检查
        checkOdsDuplicateKeys(spark, processDate)
        
        true
      } match {
        case Success(result) => result
        case Failure(e) => 
          logger.error(s"ODS层数据质量检查失败: ${e.getMessage}", e)
          false
      }
      
      val dwdCheckSuccess = Try {
        // DWD层数据完整性检查
        checkDwdCompleteness(spark, processDate)
        
        // DWD层数据一致性检查
        checkDwdConsistency(spark, processDate)
        
        true
      } match {
        case Success(result) => result
        case Failure(e) => 
          logger.error(s"DWD层数据质量检查失败: ${e.getMessage}", e)
          false
      }
      
      val dwsCheckSuccess = Try {
        // DWS层数据准确性检查
        checkDwsAccuracy(spark, processDate)
        
        true
      } match {
        case Success(result) => result
        case Failure(e) => 
          logger.error(s"DWS层数据质量检查失败: ${e.getMessage}", e)
          false
      }
      
      // 统计检查结果
      val dateStr = DateUtils.formatDate(processDate)
      val highSeverityIssues = spark.sql(
        s"""
           |SELECT COUNT(*) FROM dq_check_results
           |WHERE dt = '$dateStr' AND severity = 'high' AND check_result > 0
         """.stripMargin).first().getLong(0)
      
      val mediumSeverityIssues = spark.sql(
        s"""
           |SELECT COUNT(*) FROM dq_check_results
           |WHERE dt = '$dateStr' AND severity = 'medium' AND check_result > 0
         """.stripMargin).first().getLong(0)
      
      val lowSeverityIssues = spark.sql(
        s"""
           |SELECT COUNT(*) FROM dq_check_results
           |WHERE dt = '$dateStr' AND severity = 'low' AND check_result > 0
         """.stripMargin).first().getLong(0)
      
      logger.info(s"数据质量检查完成，发现高严重性问题: $highSeverityIssues, 中严重性问题: $mediumSeverityIssues, 低严重性问题: $lowSeverityIssues")
      
      // 根据配置的阈值判断是否通过
      val success = highSeverityIssues <= ConfigUtils.DQ_HIGH_SEVERITY_THRESHOLD && 
                   mediumSeverityIssues <= ConfigUtils.DQ_MEDIUM_SEVERITY_THRESHOLD &&
                   lowSeverityIssues <= ConfigUtils.DQ_LOW_SEVERITY_THRESHOLD
      
      // 高严重性问题超过阈值，则返回失败
      if (highSeverityIssues > ConfigUtils.DQ_HIGH_SEVERITY_THRESHOLD) {
        logger.error(s"数据质量检查发现高严重性问题超过阈值(${ConfigUtils.DQ_HIGH_SEVERITY_THRESHOLD}): $highSeverityIssues")
      }
      
      if (success) {
        logger.info(s"数据质量检查通过, 处理日期: $processDate")
      } else {
        logger.error(s"数据质量检查未通过, 处理日期: $processDate")
      }
      
      success
    } catch {
      case e: Exception =>
        logger.error(s"数据质量检查发生异常: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 创建检查结果表
   */
  private def createCheckResultsTable(spark: SparkSession): Unit = {
    logger.info("开始创建检查结果表")
    
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dq_check_results (
         |  check_id STRING COMMENT '检查ID',
         |  check_table STRING COMMENT '被检查表名',
         |  check_type STRING COMMENT '检查类型',
         |  check_desc STRING COMMENT '检查描述',
         |  check_sql STRING COMMENT '检查SQL',
         |  check_result BIGINT COMMENT '检查结果',
         |  severity STRING COMMENT '严重性：high, medium, low',
         |  check_time TIMESTAMP COMMENT '检查时间',
         |  threshold BIGINT COMMENT '阈值',
         |  status STRING COMMENT '状态：pass, warn, fail'
         |)
         |COMMENT '数据质量检查结果表'
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
       """.stripMargin)
    
    logger.info("检查结果表创建完成")
  }
  
  /**
   * 检查ODS层空值
   */
  private def checkOdsNullValues(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始检查ODS层空值")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 用户表的关键字段空值检查
    runCheck(
      spark,
      processDate,
      "ods_user_null_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_user",
      "null_check",
      "用户表关键字段空值检查",
      s"""
         |SELECT COUNT(*) FROM ${ConfigUtils.ODS_DATABASE}.ods_user
         |WHERE dt = '$dateStr' AND (user_id IS NULL OR user_name IS NULL)
       """.stripMargin,
      "high",
      0
    )
    
    // 产品表的关键字段空值检查
    runCheck(
      spark,
      processDate,
      "ods_product_null_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_product",
      "null_check",
      "产品表关键字段空值检查",
      s"""
         |SELECT COUNT(*) FROM ${ConfigUtils.ODS_DATABASE}.ods_product
         |WHERE dt = '$dateStr' AND (product_id IS NULL OR product_name IS NULL OR category IS NULL)
       """.stripMargin,
      "high",
      0
    )
    
    // 销售订单表的关键字段空值检查
    runCheck(
      spark,
      processDate,
      "ods_sales_order_null_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_sales_order",
      "null_check",
      "销售订单表关键字段空值检查",
      s"""
         |SELECT COUNT(*) FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order
         |WHERE dt = '$dateStr' AND (order_id IS NULL OR user_id IS NULL OR order_date IS NULL)
       """.stripMargin,
      "high",
      0
    )
    
    // 销售订单明细表的关键字段空值检查
    runCheck(
      spark,
      processDate,
      "ods_sales_order_item_null_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_sales_order_item",
      "null_check",
      "销售订单明细表关键字段空值检查",
      s"""
         |SELECT COUNT(*) FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item
         |WHERE dt = '$dateStr' AND (order_item_id IS NULL OR order_id IS NULL OR product_id IS NULL)
       """.stripMargin,
      "high",
      0
    )
    
    logger.info("ODS层空值检查完成")
  }
  
  /**
   * 检查ODS层记录数
   */
  private def checkOdsRecordCount(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始检查ODS层记录数")
    
    val dateStr = DateUtils.formatDate(processDate)
    val yesterday = DateUtils.formatDate(DateUtils.addDays(processDate, -1))
    
    // 用户表记录数同比检查（相比昨天不应减少超过10%）
    runCheck(
      spark,
      processDate,
      "ods_user_count_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_user",
      "count_check",
      "用户表记录数同比检查",
      s"""
         |SELECT 
         |  CASE 
         |    WHEN today_count < yesterday_count * 0.9 THEN 1
         |    ELSE 0
         |  END
         |FROM (
         |  SELECT
         |    COUNT(*) AS today_count,
         |    (SELECT COUNT(*) FROM ${ConfigUtils.ODS_DATABASE}.ods_user WHERE dt = '$yesterday') AS yesterday_count
         |  FROM ${ConfigUtils.ODS_DATABASE}.ods_user
         |  WHERE dt = '$dateStr'
         |) t
       """.stripMargin,
      "medium",
      0
    )
    
    // 产品表记录数同比检查（相比昨天不应减少超过10%）
    runCheck(
      spark,
      processDate,
      "ods_product_count_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_product",
      "count_check",
      "产品表记录数同比检查",
      s"""
         |SELECT 
         |  CASE 
         |    WHEN today_count < yesterday_count * 0.9 THEN 1
         |    ELSE 0
         |  END
         |FROM (
         |  SELECT
         |    COUNT(*) AS today_count,
         |    (SELECT COUNT(*) FROM ${ConfigUtils.ODS_DATABASE}.ods_product WHERE dt = '$yesterday') AS yesterday_count
         |  FROM ${ConfigUtils.ODS_DATABASE}.ods_product
         |  WHERE dt = '$dateStr'
         |) t
       """.stripMargin,
      "medium",
      0
    )
    
    logger.info("ODS层记录数检查完成")
  }
  
  /**
   * 检查ODS层重复主键
   */
  private def checkOdsDuplicateKeys(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始检查ODS层重复主键")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 用户表主键唯一性检查
    runCheck(
      spark,
      processDate,
      "ods_user_duplicate_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_user",
      "duplicate_check",
      "用户表主键唯一性检查",
      s"""
         |SELECT COUNT(*) - COUNT(DISTINCT user_id) 
         |FROM ${ConfigUtils.ODS_DATABASE}.ods_user
         |WHERE dt = '$dateStr'
       """.stripMargin,
      "high",
      0
    )
    
    // 产品表主键唯一性检查
    runCheck(
      spark,
      processDate,
      "ods_product_duplicate_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_product",
      "duplicate_check",
      "产品表主键唯一性检查",
      s"""
         |SELECT COUNT(*) - COUNT(DISTINCT product_id) 
         |FROM ${ConfigUtils.ODS_DATABASE}.ods_product
         |WHERE dt = '$dateStr'
       """.stripMargin,
      "high",
      0
    )
    
    // 销售订单表主键唯一性检查
    runCheck(
      spark,
      processDate,
      "ods_sales_order_duplicate_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_sales_order",
      "duplicate_check",
      "销售订单表主键唯一性检查",
      s"""
         |SELECT COUNT(*) - COUNT(DISTINCT order_id) 
         |FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order
         |WHERE dt = '$dateStr'
       """.stripMargin,
      "high",
      0
    )
    
    // 销售订单明细表主键唯一性检查
    runCheck(
      spark,
      processDate,
      "ods_sales_order_item_duplicate_check",
      s"${ConfigUtils.ODS_DATABASE}.ods_sales_order_item",
      "duplicate_check",
      "销售订单明细表主键唯一性检查",
      s"""
         |SELECT COUNT(*) - COUNT(DISTINCT order_item_id) 
         |FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item
         |WHERE dt = '$dateStr'
       """.stripMargin,
      "high",
      0
    )
    
    logger.info("ODS层重复主键检查完成")
  }
  
  /**
   * 检查DWD层数据完整性
   */
  private def checkDwdCompleteness(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始检查DWD层数据完整性")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 检查维度表用户ID是否都能在ODS层找到
    runCheck(
      spark,
      processDate,
      "dwd_dim_user_completeness",
      s"${ConfigUtils.DWD_DATABASE}.dim_user",
      "completeness",
      "用户维度表完整性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.dim_user a
         |LEFT JOIN ${ConfigUtils.ODS_DATABASE}.ods_user b ON a.user_id = b.user_id AND b.dt = '$dateStr'
         |WHERE a.dt = '$dateStr' AND b.user_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    // 检查维度表产品ID是否都能在ODS层找到
    runCheck(
      spark,
      processDate,
      "dwd_dim_product_completeness",
      s"${ConfigUtils.DWD_DATABASE}.dim_product",
      "completeness",
      "产品维度表完整性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.dim_product a
         |LEFT JOIN ${ConfigUtils.ODS_DATABASE}.ods_product b ON a.product_id = b.product_id AND b.dt = '$dateStr'
         |WHERE a.dt = '$dateStr' AND b.product_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    // 检查销售订单事实表中的订单ID是否都能在ODS层找到
    runCheck(
      spark,
      processDate,
      "dwd_fact_sales_order_completeness",
      s"${ConfigUtils.DWD_DATABASE}.fact_sales_order",
      "completeness",
      "销售订单事实表完整性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
         |LEFT JOIN ${ConfigUtils.ODS_DATABASE}.ods_sales_order b ON a.order_id = b.order_id AND b.dt = '$dateStr'
         |WHERE a.dt = '$dateStr' AND b.order_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    // 检查销售订单明细事实表中的订单项ID是否都能在ODS层找到
    runCheck(
      spark,
      processDate,
      "dwd_fact_sales_order_item_completeness",
      s"${ConfigUtils.DWD_DATABASE}.fact_sales_order_item",
      "completeness",
      "销售订单明细事实表完整性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item a
         |LEFT JOIN ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item b ON a.order_item_id = b.order_item_id AND b.dt = '$dateStr'
         |WHERE a.dt = '$dateStr' AND b.order_item_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    logger.info("DWD层数据完整性检查完成")
  }
  
  /**
   * 检查DWD层数据一致性
   */
  private def checkDwdConsistency(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始检查DWD层数据一致性")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 检查销售订单和订单明细的关联一致性
    runCheck(
      spark,
      processDate,
      "dwd_sales_order_consistency",
      s"${ConfigUtils.DWD_DATABASE}.fact_sales_order",
      "consistency",
      "销售订单和明细关联一致性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
         |LEFT JOIN (
         |  SELECT DISTINCT order_id
         |  FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item
         |  WHERE dt = '$dateStr'
         |) b ON a.order_id = b.order_id
         |WHERE a.dt = '$dateStr' AND b.order_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    // 检查销售订单明细与产品维度的关联一致性
    runCheck(
      spark,
      processDate,
      "dwd_sales_order_item_product_consistency",
      s"${ConfigUtils.DWD_DATABASE}.fact_sales_order_item",
      "consistency",
      "销售订单明细与产品关联一致性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item a
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.dim_product b ON a.product_id = b.product_id AND b.dt = '$dateStr'
         |WHERE a.dt = '$dateStr' AND b.product_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    // 检查销售订单与用户维度的关联一致性
    runCheck(
      spark,
      processDate,
      "dwd_sales_order_user_consistency",
      s"${ConfigUtils.DWD_DATABASE}.fact_sales_order",
      "consistency",
      "销售订单与用户关联一致性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.dim_user b ON a.user_id = b.user_id AND b.dt = '$dateStr'
         |WHERE a.dt = '$dateStr' AND b.user_id IS NULL
       """.stripMargin,
      "high",
      0
    )
    
    // 检查销售订单金额与明细金额一致性
    runCheck(
      spark,
      processDate,
      "dwd_sales_order_amount_consistency",
      s"${ConfigUtils.DWD_DATABASE}.fact_sales_order",
      "consistency",
      "销售订单金额与明细金额一致性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM (
         |  SELECT 
         |    a.order_id, 
         |    a.order_amount, 
         |    SUM(b.item_amount) AS total_item_amount
         |  FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item b ON a.order_id = b.order_id AND a.dt = b.dt
         |  WHERE a.dt = '$dateStr'
         |  GROUP BY a.order_id, a.order_amount
         |  HAVING ABS(a.order_amount - SUM(b.item_amount)) > 0.01
         |) t
       """.stripMargin,
      "medium",
      0
    )
    
    logger.info("DWD层数据一致性检查完成")
  }
  
  /**
   * 检查DWS层数据准确性
   */
  private def checkDwsAccuracy(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始检查DWS层数据准确性")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 检查每日销售汇总数据的准确性
    runCheck(
      spark,
      processDate,
      "dws_sales_day_accuracy",
      s"${ConfigUtils.DWS_DATABASE}.dws_sales_day",
      "accuracy",
      "每日销售汇总数据的准确性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM (
         |  SELECT 
         |    a.dt, 
         |    a.order_count AS dws_order_count,
         |    COUNT(DISTINCT b.order_id) AS fact_order_count
         |  FROM ${ConfigUtils.DWS_DATABASE}.dws_sales_day a
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order b 
         |    ON a.dt = SUBSTR(b.order_date, 1, 10) AND b.dt = '$dateStr'
         |  WHERE a.process_dt = '$dateStr'
         |  GROUP BY a.dt, a.order_count
         |  HAVING a.order_count != COUNT(DISTINCT b.order_id)
         |) t
       """.stripMargin,
      "high",
      0
    )
    
    // 检查每日销售汇总金额的准确性
    runCheck(
      spark,
      processDate,
      "dws_sales_day_amount_accuracy",
      s"${ConfigUtils.DWS_DATABASE}.dws_sales_day",
      "accuracy",
      "每日销售汇总金额的准确性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM (
         |  SELECT 
         |    a.dt, 
         |    a.order_amount AS dws_order_amount,
         |    SUM(b.order_amount) AS fact_order_amount
         |  FROM ${ConfigUtils.DWS_DATABASE}.dws_sales_day a
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order b 
         |    ON a.dt = SUBSTR(b.order_date, 1, 10) AND b.dt = '$dateStr'
         |  WHERE a.process_dt = '$dateStr'
         |  GROUP BY a.dt, a.order_amount
         |  HAVING ABS(a.order_amount - SUM(b.order_amount)) > 0.01
         |) t
       """.stripMargin,
      "high",
      0
    )
    
    // 检查产品销售汇总数据的准确性
    runCheck(
      spark,
      processDate,
      "dws_product_sales_accuracy",
      s"${ConfigUtils.DWS_DATABASE}.dws_product_sales",
      "accuracy",
      "产品销售汇总数据的准确性检查",
      s"""
         |SELECT COUNT(*) 
         |FROM (
         |  SELECT 
         |    a.product_id,
         |    a.sales_count AS dws_sales_count,
         |    SUM(b.quantity) AS fact_sales_count
         |  FROM ${ConfigUtils.DWS_DATABASE}.dws_product_sales a
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item b ON a.product_id = b.product_id AND b.dt = '$dateStr'
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order c ON b.order_id = c.order_id AND c.dt = '$dateStr'
         |  WHERE a.dt = '$dateStr'
         |    AND c.order_date >= DATE_SUB('$dateStr', 30)
         |  GROUP BY a.product_id, a.sales_count
         |  HAVING a.sales_count != SUM(b.quantity)
         |) t
       """.stripMargin,
      "medium",
      0
    )
    
    logger.info("DWS层数据准确性检查完成")
  }
  
  /**
   * 执行检查并写入结果
   */
  private def runCheck(
    spark: SparkSession, 
    processDate: String, 
    checkId: String,
    checkTable: String,
    checkType: String,
    checkDesc: String,
    checkSql: String,
    severity: String,
    threshold: Long
  ): Unit = {
    try {
      logger.info(s"执行检查: $checkDesc")
      
      val dateStr = DateUtils.formatDate(processDate)
      
      // 执行检查SQL
      val result = spark.sql(checkSql).first().getLong(0)
      
      // 判断状态
      val status = if (result <= threshold) "pass" else if (severity == "high") "fail" else "warn"
      
      // 写入检查结果
      spark.sql(
        s"""
           |INSERT INTO dq_check_results PARTITION(dt='$dateStr')
           |SELECT
           |  '$checkId' as check_id,
           |  '$checkTable' as check_table,
           |  '$checkType' as check_type,
           |  '$checkDesc' as check_desc,
           |  '$checkSql' as check_sql,
           |  $result as check_result,
           |  '$severity' as severity,
           |  current_timestamp() as check_time,
           |  $threshold as threshold,
           |  '$status' as status
         """.stripMargin)
      
      if (status == "pass") {
        logger.info(s"检查通过: $checkDesc")
      } else if (status == "warn") {
        logger.warn(s"检查警告: $checkDesc, 结果: $result, 阈值: $threshold")
      } else {
        logger.error(s"检查失败: $checkDesc, 结果: $result, 阈值: $threshold")
      }
    } catch {
      case e: Exception =>
        logger.error(s"执行检查异常: $checkDesc, ${e.getMessage}", e)
        
        // 异常也记录到结果表
        val dateStr = DateUtils.formatDate(processDate)
        spark.sql(
          s"""
             |INSERT INTO dq_check_results PARTITION(dt='$dateStr')
             |SELECT
             |  '$checkId' as check_id,
             |  '$checkTable' as check_table,
             |  '$checkType' as check_type,
             |  '$checkDesc' as check_desc,
             |  '$checkSql' as check_sql,
             |  -1 as check_result,
             |  '$severity' as severity,
             |  current_timestamp() as check_time,
             |  $threshold as threshold,
             |  'error' as status
           """.stripMargin)
    }
  }
} 