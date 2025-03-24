package com.sales.etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Logger
import com.sales.utils.{ConfigUtils, DateUtils}
import scala.util.{Try, Success, Failure}

/**
 * ODS到DWD层的数据转换
 * 负责将ODS层数据加工转换到DWD层，包括维度表和事实表的处理
 */
object OdsToDwd {
  private val logger = Logger.getLogger(this.getClass)
  
  /**
   * 执行ODS到DWD的数据转换
   * @param spark SparkSession实例
   * @param processDate 处理日期，格式：yyyyMMdd
   * @return 转换是否成功
   */
  def process(spark: SparkSession, processDate: String): Boolean = {
    logger.info(s"开始执行ODS到DWD层数据转换, 处理日期: $processDate")
    
    try {
      val dateStr = DateUtils.formatDate(processDate)
      
      // 确保DWD库存在
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${ConfigUtils.DWD_DATABASE}")
      spark.sql(s"USE ${ConfigUtils.DWD_DATABASE}")
      
      // 数据一致性验证
      val consistencyCheckPassed = validateConsistency(spark, processDate)
      if (!consistencyCheckPassed) {
        logger.error(s"ODS层数据一致性验证失败，中止处理, 处理日期: $processDate")
        return false
      }
      
      // 处理维度表
      val dimUserSuccess = processDimUser(spark, processDate)
      val dimProductSuccess = processDimProduct(spark, processDate)
      val dimDateSuccess = processDimDate(spark, processDate)
      
      // 处理事实表
      val factOrderSuccess = processFactSalesOrder(spark, processDate)
      val factOrderItemSuccess = processFactSalesOrderItem(spark, processDate)
      
      val success = dimUserSuccess && dimProductSuccess && dimDateSuccess && 
                    factOrderSuccess && factOrderItemSuccess
      
      if (success) {
        logger.info(s"ODS到DWD层数据转换完成, 处理日期: $processDate")
      } else {
        logger.error(s"ODS到DWD层数据转换失败, 处理日期: $processDate")
      }
      
      success
    } catch {
      case e: Exception =>
        logger.error(s"ODS到DWD层数据转换发生异常: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 验证ODS层数据一致性
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 验证是否通过
   */
  private def validateConsistency(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始验证ODS层数据一致性")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 检查订单和订单明细的一致性
      val orderItemConsistencyCheck = spark.sql(
        s"""
           |SELECT 
           |  COUNT(*) AS inconsistent_count
           |FROM (
           |  SELECT a.order_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order a
           |  WHERE a.dt = '$dateStr'
           |  EXCEPT
           |  SELECT DISTINCT b.order_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item b
           |  WHERE b.dt = '$dateStr'
           |) t
         """.stripMargin).first().getLong(0)
      
      if (orderItemConsistencyCheck > 0) {
        logger.error(s"订单和订单明细一致性检查失败：发现 $orderItemConsistencyCheck 个订单没有对应的订单明细")
        return false
      }
      
      // 检查产品引用一致性
      val productConsistencyCheck = spark.sql(
        s"""
           |SELECT 
           |  COUNT(*) AS inconsistent_count
           |FROM (
           |  SELECT DISTINCT product_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item
           |  WHERE dt = '$dateStr'
           |  EXCEPT
           |  SELECT product_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_product
           |  WHERE dt = '$dateStr'
           |) t
         """.stripMargin).first().getLong(0)
      
      if (productConsistencyCheck > 0) {
        logger.error(s"产品引用一致性检查失败：发现 $productConsistencyCheck 个订单明细引用了不存在的产品")
        return false
      }
      
      // 检查用户引用一致性
      val userConsistencyCheck = spark.sql(
        s"""
           |SELECT 
           |  COUNT(*) AS inconsistent_count
           |FROM (
           |  SELECT DISTINCT user_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order
           |  WHERE dt = '$dateStr'
           |  EXCEPT
           |  SELECT user_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_user
           |  WHERE dt = '$dateStr'
           |) t
         """.stripMargin).first().getLong(0)
      
      if (userConsistencyCheck > 0) {
        logger.error(s"用户引用一致性检查失败：发现 $userConsistencyCheck 个订单引用了不存在的用户")
        return false
      }
      
      // 检查类别引用一致性
      val categoryConsistencyCheck = spark.sql(
        s"""
           |SELECT 
           |  COUNT(*) AS inconsistent_count
           |FROM (
           |  SELECT DISTINCT category_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_product
           |  WHERE dt = '$dateStr'
           |  EXCEPT
           |  SELECT category_id
           |  FROM ${ConfigUtils.ODS_DATABASE}.ods_category
           |  WHERE dt = '$dateStr'
           |) t
         """.stripMargin).first().getLong(0)
      
      if (categoryConsistencyCheck > 0) {
        logger.error(s"类别引用一致性检查失败：发现 $categoryConsistencyCheck 个产品引用了不存在的类别")
        return false
      }
      
      // 检查金额计算一致性
      val amountConsistencyCheck = spark.sql(
        s"""
           |SELECT 
           |  COUNT(*) AS inconsistent_count
           |FROM (
           |  SELECT 
           |    a.order_id,
           |    a.order_amount,
           |    SUM(b.item_amount) AS total_item_amount
           |  FROM 
           |    ${ConfigUtils.ODS_DATABASE}.ods_sales_order a
           |  JOIN 
           |    ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item b
           |  ON 
           |    a.order_id = b.order_id AND a.dt = b.dt
           |  WHERE 
           |    a.dt = '$dateStr'
           |  GROUP BY 
           |    a.order_id, a.order_amount
           |  HAVING 
           |    ABS(a.order_amount - SUM(b.item_amount)) > 1.0
           |) t
         """.stripMargin).first().getLong(0)
      
      if (amountConsistencyCheck > 0) {
        logger.error(s"订单金额一致性检查失败：发现 $amountConsistencyCheck 个订单总额与订单明细金额之和不匹配")
        return false
      }
      
      logger.info("ODS层数据一致性验证通过")
      true
    } catch {
      case e: Exception =>
        logger.error(s"验证ODS层数据一致性失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 处理用户维度表
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 处理是否成功
   */
  private def processDimUser(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始处理用户维度表")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建用户维度表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWD_DATABASE}.dim_user (
           |  user_id BIGINT COMMENT '用户ID',
           |  user_name STRING COMMENT '用户名',
           |  email STRING COMMENT '邮箱',
           |  phone STRING COMMENT '电话',
           |  status INT COMMENT '状态：1-正常，0-禁用',
           |  create_date STRING COMMENT '创建日期',
           |  update_date STRING COMMENT '更新日期'
           |) COMMENT '用户维度表'
           |PARTITIONED BY (dt STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 处理用户维度数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWD_DATABASE}.dim_user PARTITION (dt='$dateStr')
           |SELECT
           |  user_id,
           |  user_name,
           |  email,
           |  phone,
           |  status,
           |  DATE_FORMAT(create_time, 'yyyy-MM-dd') AS create_date,
           |  DATE_FORMAT(update_time, 'yyyy-MM-dd') AS update_date
           |FROM ${ConfigUtils.ODS_DATABASE}.ods_user
           |WHERE dt = '$dateStr'
         """.stripMargin)
      
      val count = spark.sql(s"SELECT COUNT(*) FROM ${ConfigUtils.DWD_DATABASE}.dim_user WHERE dt='$dateStr'")
        .first().getLong(0)
      
      logger.info(s"用户维度表处理完成，记录数: $count")
      true
    } catch {
      case e: Exception =>
        logger.error(s"处理用户维度表失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 处理产品维度表
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 处理是否成功
   */
  private def processDimProduct(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始处理产品维度表")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建产品维度表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWD_DATABASE}.dim_product (
           |  product_id BIGINT COMMENT '产品ID',
           |  product_name STRING COMMENT '产品名称',
           |  category_id BIGINT COMMENT '类别ID',
           |  category_name STRING COMMENT '类别名称',
           |  price DECIMAL(10,2) COMMENT '价格',
           |  cost DECIMAL(10,2) COMMENT '成本',
           |  status INT COMMENT '状态：1-上架，0-下架',
           |  create_date STRING COMMENT '创建日期',
           |  update_date STRING COMMENT '更新日期'
           |) COMMENT '产品维度表'
           |PARTITIONED BY (dt STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 处理产品维度数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWD_DATABASE}.dim_product PARTITION (dt='$dateStr')
           |SELECT
           |  p.product_id,
           |  p.product_name,
           |  p.category_id,
           |  c.category_name,
           |  p.price,
           |  p.cost,
           |  p.status,
           |  DATE_FORMAT(p.create_time, 'yyyy-MM-dd') AS create_date,
           |  DATE_FORMAT(p.update_time, 'yyyy-MM-dd') AS update_date
           |FROM 
           |  ${ConfigUtils.ODS_DATABASE}.ods_product p
           |JOIN 
           |  ${ConfigUtils.ODS_DATABASE}.ods_category c
           |ON 
           |  p.category_id = c.category_id AND p.dt = c.dt
           |WHERE 
           |  p.dt = '$dateStr'
         """.stripMargin)
      
      val count = spark.sql(s"SELECT COUNT(*) FROM ${ConfigUtils.DWD_DATABASE}.dim_product WHERE dt='$dateStr'")
        .first().getLong(0)
      
      logger.info(s"产品维度表处理完成，记录数: $count")
      true
    } catch {
      case e: Exception =>
        logger.error(s"处理产品维度表失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 处理日期维度表
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 处理是否成功
   */
  private def processDimDate(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始处理日期维度表")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建日期维度表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWD_DATABASE}.dim_date (
           |  date_key STRING COMMENT '日期键, 格式:yyyy-MM-dd',
           |  year INT COMMENT '年',
           |  month INT COMMENT '月',
           |  day INT COMMENT '日',
           |  quarter INT COMMENT '季度',
           |  year_month STRING COMMENT '年月, 格式:yyyy-MM',
           |  week_of_year INT COMMENT '一年中的第几周',
           |  day_of_week INT COMMENT '一周中的第几天',
           |  is_weekend INT COMMENT '是否周末: 1-是, 0-否',
           |  is_holiday INT COMMENT '是否节假日: 1-是, 0-否'
           |) COMMENT '日期维度表'
           |PARTITIONED BY (dt STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 生成日期维度数据
      // 这里简化处理，只生成当天的日期维度数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWD_DATABASE}.dim_date PARTITION (dt='$dateStr')
           |SELECT 
           |  '$dateStr' AS date_key,
           |  YEAR('$dateStr') AS year,
           |  MONTH('$dateStr') AS month,
           |  DAY('$dateStr') AS day,
           |  QUARTER('$dateStr') AS quarter,
           |  DATE_FORMAT('$dateStr', 'yyyy-MM') AS year_month,
           |  WEEKOFYEAR('$dateStr') AS week_of_year,
           |  DAYOFWEEK('$dateStr') AS day_of_week,
           |  CASE WHEN DAYOFWEEK('$dateStr') IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
           |  0 AS is_holiday  -- 简化处理，实际应该从节假日表获取
         """.stripMargin)
      
      val count = spark.sql(s"SELECT COUNT(*) FROM ${ConfigUtils.DWD_DATABASE}.dim_date WHERE dt='$dateStr'")
        .first().getLong(0)
      
      logger.info(s"日期维度表处理完成，记录数: $count")
      true
    } catch {
      case e: Exception =>
        logger.error(s"处理日期维度表失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 处理销售订单事实表
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 处理是否成功
   */
  private def processFactSalesOrder(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始处理销售订单事实表")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建销售订单事实表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWD_DATABASE}.fact_sales_order (
           |  order_id BIGINT COMMENT '订单ID',
           |  user_id BIGINT COMMENT '用户ID',
           |  order_date STRING COMMENT '订单日期',
           |  order_amount DECIMAL(12,2) COMMENT '订单金额',
           |  discount_amount DECIMAL(10,2) COMMENT '折扣金额',
           |  coupon_amount DECIMAL(10,2) COMMENT '优惠券金额',
           |  tax_amount DECIMAL(10,2) COMMENT '税费',
           |  shipping_amount DECIMAL(10,2) COMMENT '运费',
           |  payment_method INT COMMENT '支付方式：1-支付宝，2-微信，3-银行卡',
           |  order_status INT COMMENT '订单状态：1-待付款，2-待发货，3-已发货，4-已完成，5-已取消',
           |  create_date STRING COMMENT '创建日期',
           |  update_date STRING COMMENT '更新日期',
           |  year_month STRING COMMENT '年月'
           |) COMMENT '销售订单事实表'
           |PARTITIONED BY (dt STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 处理销售订单事实数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWD_DATABASE}.fact_sales_order PARTITION (dt='$dateStr')
           |SELECT
           |  order_id,
           |  user_id,
           |  DATE_FORMAT(order_date, 'yyyy-MM-dd HH:mm:ss') AS order_date,
           |  order_amount,
           |  discount_amount,
           |  coupon_amount,
           |  tax_amount,
           |  shipping_amount,
           |  payment_method,
           |  order_status,
           |  DATE_FORMAT(create_time, 'yyyy-MM-dd') AS create_date,
           |  DATE_FORMAT(update_time, 'yyyy-MM-dd') AS update_date,
           |  DATE_FORMAT(order_date, 'yyyy-MM') AS year_month
           |FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order
           |WHERE dt = '$dateStr'
         """.stripMargin)
      
      val count = spark.sql(s"SELECT COUNT(*) FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order WHERE dt='$dateStr'")
        .first().getLong(0)
      
      logger.info(s"销售订单事实表处理完成，记录数: $count")
      true
    } catch {
      case e: Exception =>
        logger.error(s"处理销售订单事实表失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 处理销售订单明细事实表
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 处理是否成功
   */
  private def processFactSalesOrderItem(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始处理销售订单明细事实表")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建销售订单明细事实表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item (
           |  order_item_id BIGINT COMMENT '订单项ID',
           |  order_id BIGINT COMMENT '订单ID',
           |  product_id BIGINT COMMENT '产品ID',
           |  quantity INT COMMENT '数量',
           |  unit_price DECIMAL(10,2) COMMENT '单价',
           |  item_amount DECIMAL(12,2) COMMENT '项目金额',
           |  discount_amount DECIMAL(10,2) COMMENT '折扣金额',
           |  create_date STRING COMMENT '创建日期',
           |  update_date STRING COMMENT '更新日期',
           |  year_month STRING COMMENT '年月'
           |) COMMENT '销售订单明细事实表'
           |PARTITIONED BY (dt STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 处理销售订单明细事实数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item PARTITION (dt='$dateStr')
           |SELECT
           |  i.order_item_id,
           |  i.order_id,
           |  i.product_id,
           |  i.quantity,
           |  i.unit_price,
           |  i.item_amount,
           |  i.discount_amount,
           |  DATE_FORMAT(i.create_time, 'yyyy-MM-dd') AS create_date,
           |  DATE_FORMAT(i.update_time, 'yyyy-MM-dd') AS update_date,
           |  DATE_FORMAT(o.order_date, 'yyyy-MM') AS year_month
           |FROM ${ConfigUtils.ODS_DATABASE}.ods_sales_order_item i
           |JOIN ${ConfigUtils.ODS_DATABASE}.ods_sales_order o 
           |  ON i.order_id = o.order_id AND i.dt = o.dt
           |WHERE i.dt = '$dateStr'
         """.stripMargin)
      
      val count = spark.sql(s"SELECT COUNT(*) FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item WHERE dt='$dateStr'")
        .first().getLong(0)
      
      logger.info(s"销售订单明细事实表处理完成，记录数: $count")
      true
    } catch {
      case e: Exception =>
        logger.error(s"处理销售订单明细事实表失败: ${e.getMessage}", e)
        false
    }
  }
} 