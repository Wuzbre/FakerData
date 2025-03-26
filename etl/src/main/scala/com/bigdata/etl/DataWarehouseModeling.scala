package com.bigdata.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object DataWarehouseModeling {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Data Warehouse Modeling")
      .enableHiveSupport()
      .getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 获取处理日期
    val etlTime = LocalDateTime.now()
    val etlDate = etlTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))

    try {
      // 处理维度层
      processDimensions(spark, etlDate)
      
      // 处理明细层
      processDWD(spark, etlDate)
      
      // 处理汇总层
      processDWS(spark, etlDate)
      
    } catch {
      case e: Exception =>
        println(s"数据仓库建模过程中发生错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 处理维度层
   */
  def processDimensions(spark: SparkSession, etlDate: String): Unit = {
    // 处理用户维度
    processUserDim(spark, etlDate)
    
    // 处理商品维度
    processProductDim(spark, etlDate)
    
    // 处理供应商维度
    processSupplierDim(spark, etlDate)
    
    // 处理类目维度
    processCategoryDim(spark, etlDate)
    
    // 处理日期维度
    processDateDim(spark)
  }

  /**
   * 处理用户维度
   */
  def processUserDim(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dim.dim_user
      SELECT 
        user_id,
        username,
        email,
        phone,
        address,
        region,
        city,
        user_level,
        register_time,
        last_login_time,
        total_orders,
        total_amount,
        promotion_sensitivity,
        etl_time,
        dt
      FROM ods.ods_user
      WHERE dt = '${etlDate}'
      """
    )
  }

  /**
   * 处理商品维度
   */
  def processProductDim(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dim.dim_product
      SELECT 
        p.product_id,
        p.product_name,
        p.category_id,
        c.category_name,
        p.supplier_id,
        s.supplier_name,
        p.price,
        p.unit,
        p.description,
        p.popularity_level,
        p.is_seasonal,
        p.season_type,
        p.min_stock,
        p.max_stock,
        p.reorder_point,
        p.safety_stock,
        p.turnover_days,
        p.turnover_rate,
        p.sales_volume,
        p.sales_amount,
        p.avg_rating,
        p.etl_time,
        p.dt
      FROM ods.ods_product p
      LEFT JOIN ods.ods_category c ON p.category_id = c.category_id
      LEFT JOIN ods.ods_supplier s ON p.supplier_id = s.supplier_id
      WHERE p.dt = '${etlDate}'
      """
    )
  }

  /**
   * 处理供应商维度
   */
  def processSupplierDim(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dim.dim_supplier
      SELECT 
        supplier_id,
        supplier_name,
        contact_name,
        contact_phone,
        email,
        address,
        region,
        city,
        supply_capacity,
        cooperation_start_date,
        credit_score,
        supply_stability,
        avg_delivery_days,
        etl_time,
        dt
      FROM ods.ods_supplier
      WHERE dt = '${etlDate}'
      """
    )
  }

  /**
   * 处理类目维度
   */
  def processCategoryDim(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dim.dim_category
      SELECT 
        category_id,
        category_name,
        parent_id,
        category_level,
        sort_order,
        is_seasonal,
        season_type,
        sales_volume,
        sales_amount,
        etl_time,
        dt
      FROM ods.ods_category
      WHERE dt = '${etlDate}'
      """
    )
  }

  /**
   * 处理日期维度
   */
  def processDateDim(spark: SparkSession): Unit = {
    // 生成日期维度表
    spark.sql(
      """
      CREATE TABLE IF NOT EXISTS dim.dim_date (
        date_id STRING,
        full_date DATE,
        year INT,
        month INT,
        day INT,
        quarter INT,
        week_of_year INT,
        day_of_week INT,
        is_weekend BOOLEAN,
        is_holiday BOOLEAN,
        holiday_name STRING,
        season STRING
      )
      """
    )
    
    // 这里需要添加生成日期维度数据的逻辑
  }

  /**
   * 处理DWD层
   */
  def processDWD(spark: SparkSession, etlDate: String): Unit = {
    // 处理订单事实表
    processOrderFact(spark, etlDate)
    
    // 处理订单明细事实表
    processOrderDetailFact(spark, etlDate)
    
    // 处理库存变动事实表
    processInventoryChangeFact(spark, etlDate)
    
    // 处理价格变动事实表
    processPriceChangeFact(spark, etlDate)
    
    // 处理促销事实表
    processPromotionFact(spark, etlDate)
  }

  /**
   * 处理订单事实表
   */
  def processOrderFact(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dwd.dwd_fact_order
      SELECT 
        o.order_id,
        o.user_id,
        o.order_status,
        o.total_amount,
        o.actual_amount,
        o.payment_method,
        o.shipping_address,
        o.shipping_phone,
        o.shipping_name,
        o.region,
        o.city,
        o.order_source,
        o.promotion_id,
        o.promotion_discount,
        o.weather,
        o.delivery_days,
        o.is_first_order,
        o.created_at,
        o.updated_at,
        o.etl_time,
        o.dt
      FROM ods.ods_order o
      WHERE o.dt = '${etlDate}'
      """
    )
  }

  /**
   * 处理DWS层
   */
  def processDWS(spark: SparkSession, etlDate: String): Unit = {
    // 处理用户行为汇总
    processUserBehaviorSummary(spark, etlDate)
    
    // 处理商品销售汇总
    processProductSalesSummary(spark, etlDate)
    
    // 处理供应商表现汇总
    processSupplierPerformanceSummary(spark, etlDate)
    
    // 处理促销效果汇总
    processPromotionEffectSummary(spark, etlDate)
    
    // 处理库存周转汇总
    processInventoryTurnoverSummary(spark, etlDate)
  }

  /**
   * 处理用户行为汇总
   */
  def processUserBehaviorSummary(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dws.dws_user_behavior
      SELECT 
        u.user_id,
        u.user_level,
        COUNT(DISTINCT o.order_id) as order_count,
        SUM(o.total_amount) as total_amount,
        AVG(o.total_amount) as avg_order_amount,
        COUNT(DISTINCT CASE WHEN o.promotion_id IS NOT NULL THEN o.order_id END) as promotion_order_count,
        SUM(CASE WHEN o.promotion_id IS NOT NULL THEN o.promotion_discount END) as total_promotion_discount,
        COUNT(DISTINCT p.product_id) as purchased_product_count,
        '${etlDate}' as dt
      FROM dim.dim_user u
      LEFT JOIN dwd.dwd_fact_order o ON u.user_id = o.user_id
      LEFT JOIN dwd.dwd_fact_order_detail d ON o.order_id = d.order_id
      LEFT JOIN dim.dim_product p ON d.product_id = p.product_id
      WHERE u.dt = '${etlDate}'
      GROUP BY u.user_id, u.user_level
      """
    )
  }

  /**
   * 处理商品销售汇总
   */
  def processProductSalesSummary(spark: SparkSession, etlDate: String): Unit = {
    spark.sql(
      """
      INSERT OVERWRITE TABLE dws.dws_product_sales
      SELECT 
        p.product_id,
        p.product_name,
        p.category_id,
        c.category_name,
        COUNT(DISTINCT o.order_id) as order_count,
        SUM(d.quantity) as total_quantity,
        SUM(d.total_price) as total_amount,
        AVG(d.unit_price) as avg_price,
        SUM(CASE WHEN d.is_promotion_price THEN d.quantity END) as promotion_quantity,
        SUM(CASE WHEN d.is_promotion_price THEN d.total_price END) as promotion_amount,
        AVG(d.rating) as avg_rating,
        '${etlDate}' as dt
      FROM dim.dim_product p
      LEFT JOIN dim.dim_category c ON p.category_id = c.category_id
      LEFT JOIN dwd.dwd_fact_order_detail d ON p.product_id = d.product_id
      LEFT JOIN dwd.dwd_fact_order o ON d.order_id = o.order_id
      WHERE p.dt = '${etlDate}'
      GROUP BY p.product_id, p.product_name, p.category_id, c.category_name
      """
    )
  }
} 