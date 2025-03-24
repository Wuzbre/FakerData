package com.sales.etl

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.log4j.Logger
import com.sales.utils.{ConfigUtils, DateUtils}
import scala.util.{Try, Success, Failure}

/**
 * DWD到DWS层的数据聚合
 * 负责对DWD层的明细数据进行汇总分析，生成DWS层的汇总数据
 */
object DwdToDws {
  private val logger = Logger.getLogger(this.getClass)
  
  /**
   * 执行DWD到DWS的数据聚合
   * @param spark SparkSession实例
   * @param processDate 处理日期，格式：yyyyMMdd
   * @return 聚合是否成功
   */
  def process(spark: SparkSession, processDate: String): Boolean = {
    logger.info(s"开始执行DWD到DWS层数据聚合, 处理日期: $processDate")
    
    try {
      val dateStr = DateUtils.formatDate(processDate)
      
      // 确保DWS库存在
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${ConfigUtils.DWS_DATABASE}")
      spark.sql(s"USE ${ConfigUtils.DWS_DATABASE}")
      
      // 每日销售汇总
      val dailySalesSuccess = aggregateDailySales(spark, processDate)
      
      // 每月销售汇总
      val monthlySalesSuccess = aggregateMonthlySales(spark, processDate)
      
      // 产品销售汇总
      val productSalesSuccess = aggregateProductSales(spark, processDate)
      
      // 用户购买汇总
      val userPurchaseSuccess = aggregateUserPurchase(spark, processDate)
      
      // 类别销售汇总
      val categorySalesSuccess = aggregateCategorySales(spark, processDate)
      
      val success = dailySalesSuccess && monthlySalesSuccess && 
                    productSalesSuccess && userPurchaseSuccess && 
                    categorySalesSuccess
      
      if (success) {
        logger.info(s"DWD到DWS层数据聚合完成, 处理日期: $processDate")
      } else {
        logger.error(s"DWD到DWS层数据聚合失败, 处理日期: $processDate")
      }
      
      success
    } catch {
      case e: Exception =>
        logger.error(s"DWD到DWS层数据聚合发生异常: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 聚合每日销售数据
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 聚合是否成功
   */
  private def aggregateDailySales(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始聚合每日销售数据")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建每日销售汇总表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWS_DATABASE}.dws_sales_day (
           |  dt STRING COMMENT '日期',
           |  order_count BIGINT COMMENT '订单数',
           |  order_amount DECIMAL(20,2) COMMENT '订单金额',
           |  order_user_count BIGINT COMMENT '下单用户数',
           |  order_item_count BIGINT COMMENT '订单项数',
           |  discount_amount DECIMAL(20,2) COMMENT '折扣金额',
           |  process_dt STRING COMMENT '处理日期'
           |) COMMENT '每日销售汇总表'
           |PARTITIONED BY (dt_type STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 计算最近30天的每日销售数据
      val today = DateUtils.formatDate(processDate)
      val thirtyDaysAgo = DateUtils.formatDate(DateUtils.addDays(processDate, -30))
      
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWS_DATABASE}.dws_sales_day PARTITION(dt_type='day')
           |SELECT
           |  substr(a.order_date, 1, 10) as dt,
           |  COUNT(DISTINCT a.order_id) as order_count,
           |  SUM(a.order_amount) as order_amount,
           |  COUNT(DISTINCT a.user_id) as order_user_count,
           |  COUNT(b.order_item_id) as order_item_count,
           |  SUM(a.discount_amount) as discount_amount,
           |  '$dateStr' as process_dt
           |FROM 
           |  ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
           |JOIN 
           |  ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item b
           |ON 
           |  a.order_id = b.order_id AND a.dt = b.dt
           |WHERE 
           |  a.dt = '$dateStr'
           |  AND substr(a.order_date, 1, 10) >= '$thirtyDaysAgo'
           |  AND substr(a.order_date, 1, 10) <= '$today'
           |GROUP BY 
           |  substr(a.order_date, 1, 10)
         """.stripMargin)
      
      // 验证是否生成了数据
      val count = spark.sql(
        s"""
           |SELECT COUNT(*) FROM ${ConfigUtils.DWS_DATABASE}.dws_sales_day 
           |WHERE dt_type = 'day' AND process_dt = '$dateStr'
         """.stripMargin).first().getLong(0)
      
      logger.info(s"每日销售汇总数据聚合完成，生成记录数: $count")
      count > 0
    } catch {
      case e: Exception =>
        logger.error(s"聚合每日销售数据失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 聚合每月销售数据
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 聚合是否成功
   */
  private def aggregateMonthlySales(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始聚合每月销售数据")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建每月销售汇总表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWS_DATABASE}.dws_sales_month (
           |  year_month STRING COMMENT '年月',
           |  order_count BIGINT COMMENT '订单数',
           |  order_amount DECIMAL(20,2) COMMENT '订单金额',
           |  order_user_count BIGINT COMMENT '下单用户数',
           |  order_item_count BIGINT COMMENT '订单项数',
           |  discount_amount DECIMAL(20,2) COMMENT '折扣金额',
           |  process_dt STRING COMMENT '处理日期'
           |) COMMENT '每月销售汇总表'
           |PARTITIONED BY (dt_type STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 计算最近12个月的每月销售数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWS_DATABASE}.dws_sales_month PARTITION(dt_type='month')
           |SELECT
           |  substr(a.order_date, 1, 7) as year_month,
           |  COUNT(DISTINCT a.order_id) as order_count,
           |  SUM(a.order_amount) as order_amount,
           |  COUNT(DISTINCT a.user_id) as order_user_count,
           |  COUNT(b.order_item_id) as order_item_count,
           |  SUM(a.discount_amount) as discount_amount,
           |  '$dateStr' as process_dt
           |FROM 
           |  ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
           |JOIN 
           |  ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item b
           |ON 
           |  a.order_id = b.order_id AND a.dt = b.dt
           |WHERE 
           |  a.dt = '$dateStr'
           |  AND a.year_month >= date_format(add_months(to_date('$dateStr', 'yyyy-MM-dd'), -12), 'yyyy-MM')
           |GROUP BY 
           |  substr(a.order_date, 1, 7)
         """.stripMargin)
      
      // 验证是否生成了数据
      val count = spark.sql(
        s"""
           |SELECT COUNT(*) FROM ${ConfigUtils.DWS_DATABASE}.dws_sales_month 
           |WHERE dt_type = 'month' AND process_dt = '$dateStr'
         """.stripMargin).first().getLong(0)
      
      logger.info(s"每月销售汇总数据聚合完成，生成记录数: $count")
      count > 0
    } catch {
      case e: Exception =>
        logger.error(s"聚合每月销售数据失败: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 聚合产品销售数据
   * @param spark SparkSession实例
   * @param processDate 处理日期
   * @return 聚合是否成功
   */
  private def aggregateProductSales(spark: SparkSession, processDate: String): Boolean = {
    logger.info("开始聚合产品销售数据")
    
    val dateStr = DateUtils.formatDate(processDate)
    
    try {
      // 创建产品销售汇总表
      spark.sql(
        s"""
           |CREATE TABLE IF NOT EXISTS ${ConfigUtils.DWS_DATABASE}.dws_product_sales (
           |  product_id BIGINT COMMENT '产品ID',
           |  product_name STRING COMMENT '产品名称',
           |  category_id BIGINT COMMENT '类别ID',
           |  category_name STRING COMMENT '类别名称',
           |  sales_count BIGINT COMMENT '销售数量',
           |  sales_amount DECIMAL(20,2) COMMENT '销售金额',
           |  discount_amount DECIMAL(20,2) COMMENT '折扣金额',
           |  order_count BIGINT COMMENT '订单数',
           |  user_count BIGINT COMMENT '购买用户数',
           |  process_dt STRING COMMENT '处理日期'
           |) COMMENT '产品销售汇总表'
           |PARTITIONED BY (dt STRING)
           |STORED AS PARQUET
         """.stripMargin)
      
      // 聚合产品销售数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE ${ConfigUtils.DWS_DATABASE}.dws_product_sales PARTITION(dt='$dateStr')
           |SELECT
           |  p.product_id,
           |  p.product_name,
           |  p.category_id,
           |  p.category_name,
           |  SUM(i.quantity) as sales_count,
           |  SUM(i.item_amount) as sales_amount,
           |  SUM(i.discount_amount) as discount_amount,
           |  COUNT(DISTINCT i.order_id) as order_count,
           |  COUNT(DISTINCT o.user_id) as user_count,
           |  '$dateStr' as process_dt
           |FROM 
           |  ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item i
           |JOIN 
           |  ${ConfigUtils.DWD_DATABASE}.fact_sales_order o
           |ON 
           |  i.order_id = o.order_id AND i.dt = o.dt
           |JOIN 
           |  ${ConfigUtils.DWD_DATABASE}.dim_product p
           |ON 
           |  i.product_id = p.product_id AND i.dt = p.dt
           |WHERE 
           |  i.dt = '$dateStr'
           |GROUP BY 
           |  p.product_id, p.product_name, p.category_id, p.category_name
         """.stripMargin)
      
      // 验证是否生成了数据
      val count = spark.sql(
        s"""
           |SELECT COUNT(*) FROM ${ConfigUtils.DWS_DATABASE}.dws_product_sales 
           |WHERE dt = '$dateStr'
         """.stripMargin).first().getLong(0)
      
      logger.info(s"产品销售汇总数据聚合完成，生成记录数: $count")
      count > 0
    } catch {
      case e: Exception =>
    // 创建每日销售汇总表（如果不存在）
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dws_sales_day (
         |  dt STRING COMMENT '日期',
         |  order_count BIGINT COMMENT '订单数',
         |  order_amount DECIMAL(20,2) COMMENT '订单总金额',
         |  paid_order_count BIGINT COMMENT '已支付订单数',
         |  paid_amount DECIMAL(20,2) COMMENT '已支付金额',
         |  cancelled_order_count BIGINT COMMENT '取消订单数',
         |  user_count BIGINT COMMENT '下单用户数',
         |  new_user_count BIGINT COMMENT '新用户下单数',
         |  product_count BIGINT COMMENT '销售商品种类数',
         |  promotion_order_count BIGINT COMMENT '促销订单数',
         |  promotion_amount DECIMAL(20,2) COMMENT '促销订单金额',
         |  create_time TIMESTAMP COMMENT '创建时间'
         |)
         |COMMENT '每日销售汇总表'
         |PARTITIONED BY (process_dt STRING)
         |STORED AS PARQUET
       """.stripMargin)
    
    val dateStr = DateUtils.formatDate(processDate)
    val monthStart = DateUtils.getMonthFirstDay(processDate)
    val lastMonthStart = DateUtils.getLastMonthFirstDay(processDate)
    
    // 分析过去30天数据，按天汇总
    for (i <- 0 until 30) {
      val curDate = DateUtils.addDays(processDate, -i)
      val curDateStr = DateUtils.formatDate(curDate)
      
      logger.info(s"正在处理日期: $curDateStr 的销售数据汇总")
      
      // 从DWD层聚合当天销售数据
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE dws_sales_day PARTITION(process_dt='$dateStr')
           |SELECT
           |  '$curDateStr' as dt,
           |  COUNT(DISTINCT a.order_id) as order_count,
           |  SUM(a.order_amount) as order_amount,
           |  COUNT(DISTINCT CASE WHEN a.order_status IN ('paid', 'shipped', 'delivered') THEN a.order_id ELSE NULL END) as paid_order_count,
           |  SUM(CASE WHEN a.order_status IN ('paid', 'shipped', 'delivered') THEN a.order_amount ELSE 0 END) as paid_amount,
           |  COUNT(DISTINCT CASE WHEN a.order_status = 'cancelled' THEN a.order_id ELSE NULL END) as cancelled_order_count,
           |  COUNT(DISTINCT a.user_id) as user_count,
           |  COUNT(DISTINCT CASE WHEN u.register_date = '$curDateStr' THEN a.user_id ELSE NULL END) as new_user_count,
           |  COUNT(DISTINCT b.product_id) as product_count,
           |  COUNT(DISTINCT CASE WHEN a.is_promotion = true THEN a.order_id ELSE NULL END) as promotion_order_count,
           |  SUM(CASE WHEN a.is_promotion = true THEN a.order_amount ELSE 0 END) as promotion_amount,
           |  current_timestamp() as create_time
           |FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
           |JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item b ON a.order_id = b.order_id AND a.dt = b.dt
           |JOIN ${ConfigUtils.DWD_DATABASE}.dim_user u ON a.user_id = u.user_id AND u.dt = '$dateStr'
           |WHERE a.dt = '$dateStr' AND SUBSTR(a.order_date, 1, 10) = '$curDateStr'
         """.stripMargin)
    }
    
    logger.info("每日销售数据汇总完成")
  }
  
  /**
   * 创建并汇总每月销售数据
   */
  private def createAndAggregateSalesMonth(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始创建并汇总每月销售数据")
    
    // 创建每月销售汇总表（如果不存在）
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dws_sales_month (
         |  month STRING COMMENT '月份',
         |  order_count BIGINT COMMENT '订单数',
         |  order_amount DECIMAL(20,2) COMMENT '订单总金额',
         |  paid_order_count BIGINT COMMENT '已支付订单数',
         |  paid_amount DECIMAL(20,2) COMMENT '已支付金额',
         |  cancelled_order_count BIGINT COMMENT '取消订单数',
         |  refund_order_count BIGINT COMMENT '退款订单数',
         |  user_count BIGINT COMMENT '下单用户数',
         |  new_user_count BIGINT COMMENT '新用户下单数',
         |  gmv DECIMAL(20,2) COMMENT '交易总额',
         |  promotion_amount DECIMAL(20,2) COMMENT '促销金额',
         |  create_time TIMESTAMP COMMENT '创建时间'
         |)
         |COMMENT '每月销售汇总表'
         |PARTITIONED BY (process_dt STRING)
         |STORED AS PARQUET
       """.stripMargin)
    
    val dateStr = DateUtils.formatDate(processDate)
    val monthStart = DateUtils.getMonthFirstDay(processDate)
    val lastMonthStart = DateUtils.getLastMonthFirstDay(processDate)
    
    // 获取当前月份和前两个月的月份
    val curMonth = DateUtils.getYearMonth(processDate)
    val lastMonth = DateUtils.getYearMonth(DateUtils.addMonths(processDate, -1))
    val lastLastMonth = DateUtils.getYearMonth(DateUtils.addMonths(processDate, -2))
    
    // 处理当前月数据
    logger.info(s"正在处理月份: $curMonth 的销售数据汇总")
    
    // 从DWD层聚合当月销售数据
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dws_sales_month PARTITION(process_dt='$dateStr')
         |SELECT
         |  month_str as month,
         |  SUM(order_count) as order_count,
         |  SUM(order_amount) as order_amount,
         |  SUM(paid_order_count) as paid_order_count,
         |  SUM(paid_amount) as paid_amount,
         |  SUM(cancelled_order_count) as cancelled_order_count,
         |  SUM(refund_order_count) as refund_order_count,
         |  COUNT(DISTINCT user_id) as user_count,
         |  SUM(new_user_count) as new_user_count,
         |  SUM(paid_amount) as gmv,
         |  SUM(promotion_amount) as promotion_amount,
         |  current_timestamp() as create_time
         |FROM (
         |  SELECT
         |    SUBSTR(a.order_date, 1, 7) as month_str,
         |    COUNT(DISTINCT a.order_id) as order_count,
         |    SUM(a.order_amount) as order_amount,
         |    COUNT(DISTINCT CASE WHEN a.order_status IN ('paid', 'shipped', 'delivered') THEN a.order_id ELSE NULL END) as paid_order_count,
         |    SUM(CASE WHEN a.order_status IN ('paid', 'shipped', 'delivered') THEN a.order_amount ELSE 0 END) as paid_amount,
         |    COUNT(DISTINCT CASE WHEN a.order_status = 'cancelled' THEN a.order_id ELSE NULL END) as cancelled_order_count,
         |    COUNT(DISTINCT CASE WHEN a.order_status = 'refunded' THEN a.order_id ELSE NULL END) as refund_order_count,
         |    a.user_id,
         |    COUNT(DISTINCT CASE WHEN u.register_date >= '$monthStart' THEN a.user_id ELSE NULL END) as new_user_count,
         |    SUM(CASE WHEN a.is_promotion = true THEN a.order_amount ELSE 0 END) as promotion_amount
         |  FROM ${ConfigUtils.DWD_DATABASE}.fact_sales_order a
         |  JOIN ${ConfigUtils.DWD_DATABASE}.dim_user u ON a.user_id = u.user_id AND u.dt = '$dateStr'
         |  WHERE a.dt = '$dateStr' AND (
         |    SUBSTR(a.order_date, 1, 7) = '$curMonth' OR 
         |    SUBSTR(a.order_date, 1, 7) = '$lastMonth' OR 
         |    SUBSTR(a.order_date, 1, 7) = '$lastLastMonth'
         |  )
         |  GROUP BY SUBSTR(a.order_date, 1, 7), a.user_id
         |) t
         |GROUP BY month_str
       """.stripMargin)
    
    logger.info("每月销售数据汇总完成")
  }
  
  /**
   * 创建并汇总产品销售数据
   */
  private def createAndAggregateProductSales(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始创建并汇总产品销售数据")
    
    // 创建产品销售汇总表（如果不存在）
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dws_product_sales (
         |  product_id STRING COMMENT '产品ID',
         |  product_name STRING COMMENT '产品名称',
         |  category STRING COMMENT '产品类别',
         |  sales_count BIGINT COMMENT '销售数量',
         |  sales_amount DECIMAL(20,2) COMMENT '销售金额',
         |  user_count BIGINT COMMENT '购买用户数',
         |  avg_price DECIMAL(10,2) COMMENT '平均售价',
         |  profit_amount DECIMAL(20,2) COMMENT '利润金额',
         |  profit_rate DECIMAL(5,2) COMMENT '利润率',
         |  return_count BIGINT COMMENT '退货数量',
         |  return_rate DECIMAL(5,2) COMMENT '退货率',
         |  create_time TIMESTAMP COMMENT '创建时间'
         |)
         |COMMENT '产品销售汇总表'
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
       """.stripMargin)
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 从DWD层聚合产品销售数据（过去30天）
    val thirtyDaysAgo = DateUtils.addDays(processDate, -30)
    val thirtyDaysAgoStr = DateUtils.formatDate(thirtyDaysAgo)
    
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dws_product_sales PARTITION(dt='$dateStr')
         |SELECT
         |  p.product_id,
         |  p.product_name,
         |  p.category,
         |  COALESCE(SUM(i.quantity), 0) as sales_count,
         |  COALESCE(SUM(i.item_amount), 0) as sales_amount,
         |  COUNT(DISTINCT o.user_id) as user_count,
         |  COALESCE(SUM(i.item_amount) / SUM(i.quantity), 0) as avg_price,
         |  COALESCE(SUM(i.item_amount) - (SUM(i.quantity) * p.cost), 0) as profit_amount,
         |  COALESCE((SUM(i.item_amount) - (SUM(i.quantity) * p.cost)) / SUM(i.item_amount) * 100, 0) as profit_rate,
         |  COALESCE(COUNT(DISTINCT CASE WHEN o.order_status = 'refunded' THEN i.order_item_id ELSE NULL END), 0) as return_count,
         |  COALESCE(COUNT(DISTINCT CASE WHEN o.order_status = 'refunded' THEN i.order_item_id ELSE NULL END) / COUNT(i.order_item_id) * 100, 0) as return_rate,
         |  current_timestamp() as create_time
         |FROM ${ConfigUtils.DWD_DATABASE}.dim_product p
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item i ON p.product_id = i.product_id
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order o ON i.order_id = o.order_id AND i.dt = o.dt
         |WHERE p.dt = '$dateStr'
         |  AND (i.dt IS NULL OR i.dt = '$dateStr')
         |  AND (o.dt IS NULL OR o.dt = '$dateStr')
         |  AND (o.order_date IS NULL OR o.order_date >= '$thirtyDaysAgoStr')
         |GROUP BY
         |  p.product_id, p.product_name, p.category, p.cost
       """.stripMargin)
    
    logger.info("产品销售数据汇总完成")
  }
  
  /**
   * 创建并汇总客户行为数据
   */
  private def createAndAggregateCustomerBehavior(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始创建并汇总客户行为数据")
    
    // 创建客户行为汇总表（如果不存在）
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dws_customer_behavior (
         |  user_id STRING COMMENT '用户ID',
         |  user_name STRING COMMENT '用户名',
         |  user_level STRING COMMENT '用户等级',
         |  order_count BIGINT COMMENT '订单数',
         |  order_amount DECIMAL(20,2) COMMENT '订单金额',
         |  first_order_date STRING COMMENT '首次下单日期',
         |  last_order_date STRING COMMENT '最近下单日期',
         |  avg_order_amount DECIMAL(10,2) COMMENT '平均订单金额',
         |  max_order_amount DECIMAL(10,2) COMMENT '最大订单金额',
         |  product_categories INT COMMENT '购买商品类别数',
         |  most_purchased_category STRING COMMENT '最常购买类别',
         |  purchase_frequency DECIMAL(5,2) COMMENT '购买频率(次/月)',
         |  customer_worth DECIMAL(10,2) COMMENT '客户价值',
         |  create_time TIMESTAMP COMMENT '创建时间'
         |)
         |COMMENT '客户行为汇总表'
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
       """.stripMargin)
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 从DWD层聚合客户行为数据（过去90天）
    val ninetyDaysAgo = DateUtils.addDays(processDate, -90)
    val ninetyDaysAgoStr = DateUtils.formatDate(ninetyDaysAgo)
    
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dws_customer_behavior PARTITION(dt='$dateStr')
         |WITH user_orders AS (
         |  SELECT
         |    u.user_id,
         |    u.user_name,
         |    u.user_level,
         |    o.order_id,
         |    o.order_date,
         |    o.order_amount,
         |    i.product_id
         |  FROM ${ConfigUtils.DWD_DATABASE}.dim_user u
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order o ON u.user_id = o.user_id
         |  JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item i ON o.order_id = i.order_id AND o.dt = i.dt
         |  WHERE u.dt = '$dateStr'
         |    AND o.dt = '$dateStr'
         |    AND i.dt = '$dateStr'
         |    AND o.order_date >= '$ninetyDaysAgoStr'
         |),
         |user_product_categories AS (
         |  SELECT
         |    uo.user_id,
         |    p.category,
         |    COUNT(*) as category_count
         |  FROM user_orders uo
         |  JOIN ${ConfigUtils.DWD_DATABASE}.dim_product p ON uo.product_id = p.product_id AND p.dt = '$dateStr'
         |  GROUP BY uo.user_id, p.category
         |),
         |user_favorite_category AS (
         |  SELECT
         |    user_id,
         |    category,
         |    category_count,
         |    ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY category_count DESC) as rn
         |  FROM user_product_categories
         |)
         |SELECT
         |  u.user_id,
         |  u.user_name,
         |  u.user_level,
         |  COUNT(DISTINCT o.order_id) as order_count,
         |  SUM(o.order_amount) as order_amount,
         |  MIN(o.order_date) as first_order_date,
         |  MAX(o.order_date) as last_order_date,
         |  COALESCE(AVG(o.order_amount), 0) as avg_order_amount,
         |  COALESCE(MAX(o.order_amount), 0) as max_order_amount,
         |  COUNT(DISTINCT p.category) as product_categories,
         |  MAX(CASE WHEN fc.rn = 1 THEN fc.category ELSE NULL END) as most_purchased_category,
         |  COALESCE(COUNT(DISTINCT o.order_id) / 3, 0) as purchase_frequency,
         |  COALESCE(SUM(o.order_amount) * (CASE 
         |    WHEN u.user_level = 'diamond' THEN 1.5
         |    WHEN u.user_level = 'platinum' THEN 1.3
         |    WHEN u.user_level = 'gold' THEN 1.2
         |    WHEN u.user_level = 'silver' THEN 1.1
         |    ELSE 1.0
         |  END), 0) as customer_worth,
         |  current_timestamp() as create_time
         |FROM ${ConfigUtils.DWD_DATABASE}.dim_user u
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order o ON u.user_id = o.user_id AND o.dt = '$dateStr' AND o.order_date >= '$ninetyDaysAgoStr'
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.fact_sales_order_item i ON o.order_id = i.order_id AND o.dt = i.dt
         |LEFT JOIN ${ConfigUtils.DWD_DATABASE}.dim_product p ON i.product_id = p.product_id AND p.dt = '$dateStr'
         |LEFT JOIN user_favorite_category fc ON u.user_id = fc.user_id
         |WHERE u.dt = '$dateStr'
         |GROUP BY u.user_id, u.user_name, u.user_level
       """.stripMargin)
    
    logger.info("客户行为数据汇总完成")
  }
  
  /**
   * 创建并汇总供应商采购数据
   */
  private def createAndAggregateSupplierPurchase(spark: SparkSession, processDate: String): Unit = {
    logger.info("开始创建并汇总供应商采购数据")
    
    // 创建供应商采购汇总表（如果不存在）
    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dws_supplier_purchase (
         |  supplier_id STRING COMMENT '供应商ID',
         |  purchase_count BIGINT COMMENT '采购单数',
         |  purchase_amount DECIMAL(20,2) COMMENT '采购金额',
         |  product_count INT COMMENT '采购产品种类数',
         |  avg_delivery_days INT COMMENT '平均交付天数',
         |  on_time_delivery_rate DECIMAL(5,2) COMMENT '按时交付率',
         |  purchase_cycle INT COMMENT '采购周期',
         |  last_purchase_date STRING COMMENT '最近采购日期',
         |  create_time TIMESTAMP COMMENT '创建时间'
         |)
         |COMMENT '供应商采购汇总表'
         |PARTITIONED BY (dt STRING)
         |STORED AS PARQUET
       """.stripMargin)
    
    val dateStr = DateUtils.formatDate(processDate)
    
    // 从DWD层聚合供应商采购数据（过去6个月）
    val sixMonthsAgo = DateUtils.addMonths(processDate, -6)
    val sixMonthsAgoStr = DateUtils.formatDate(sixMonthsAgo)
    
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE dws_supplier_purchase PARTITION(dt='$dateStr')
         |WITH supplier_delivery AS (
         |  SELECT
         |    p.supplier_id,
         |    p.purchase_id,
         |    p.expected_delivery_date,
         |    p.actual_delivery_date,
         |    DATEDIFF(to_date(p.actual_delivery_date), to_date(p.expected_delivery_date)) as delivery_diff_days,
         |    CASE 
         |      WHEN p.actual_delivery_date <= p.expected_delivery_date THEN 1 
         |      ELSE 0 
         |    END as on_time_delivery,
         |    DATEDIFF(to_date(p.actual_delivery_date), to_date(p.purchase_date)) as delivery_days
         |  FROM ${ConfigUtils.DWD_DATABASE}.fact_purchase_order p
         |  WHERE p.dt = '$dateStr'
         |    AND p.purchase_date >= '$sixMonthsAgoStr'
         |    AND p.actual_delivery_date IS NOT NULL
         |),
         |supplier_purchase_dates AS (
         |  SELECT
         |    supplier_id,
         |    purchase_date,
         |    LEAD(purchase_date) OVER(PARTITION BY supplier_id ORDER BY purchase_date) as next_purchase_date
         |  FROM ${ConfigUtils.DWD_DATABASE}.fact_purchase_order
         |  WHERE dt = '$dateStr'
         |    AND purchase_date >= '$sixMonthsAgoStr'
         |),
         |supplier_purchase_cycles AS (
         |  SELECT
         |    supplier_id,
         |    AVG(DATEDIFF(to_date(next_purchase_date), to_date(purchase_date))) as avg_cycle
         |  FROM supplier_purchase_dates
         |  WHERE next_purchase_date IS NOT NULL
         |  GROUP BY supplier_id
         |)
         |SELECT
         |  p.supplier_id,
         |  COUNT(DISTINCT p.purchase_id) as purchase_count,
         |  SUM(p.purchase_amount) as purchase_amount,
         |  COUNT(DISTINCT i.product_id) as product_count,
         |  COALESCE(CAST(AVG(sd.delivery_days) AS INT), 0) as avg_delivery_days,
         |  COALESCE(SUM(sd.on_time_delivery) * 100 / COUNT(sd.purchase_id), 0) as on_time_delivery_rate,
         |  COALESCE(CAST(spc.avg_cycle AS INT), 0) as purchase_cycle,
         |  MAX(p.purchase_date) as last_purchase_date,
         |  current_timestamp() as create_time
         |FROM ${ConfigUtils.DWD_DATABASE}.fact_purchase_order p
         |JOIN ${ConfigUtils.DWD_DATABASE}.fact_purchase_order_item i ON p.purchase_id = i.purchase_id AND p.dt = i.dt
         |LEFT JOIN supplier_delivery sd ON p.supplier_id = sd.supplier_id AND p.purchase_id = sd.purchase_id
         |LEFT JOIN supplier_purchase_cycles spc ON p.supplier_id = spc.supplier_id
         |WHERE p.dt = '$dateStr'
         |  AND i.dt = '$dateStr'
         |  AND p.purchase_date >= '$sixMonthsAgoStr'
         |GROUP BY p.supplier_id, spc.avg_cycle
       """.stripMargin)
    
    logger.info("供应商采购数据汇总完成")
  }
} 