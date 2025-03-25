package com.sales.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class SalesFactProcessor(spark: SparkSession) extends FactProcessor(spark) {
  override protected val factName: String = "sales"
  override protected val sourceTable: String = "ods_orders"
  override protected val targetTable: String = "dwd_fact_sales"
  
  override protected def createFactTable(): Unit = {
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $targetTable (
        sales_key BIGINT,
        order_id BIGINT,
        user_key BIGINT,
        product_key BIGINT,
        date_key INT,
        quantity INT,
        unit_price DECIMAL(10,2),
        total_amount DECIMAL(10,2),
        discount_amount DECIMAL(10,2),
        final_amount DECIMAL(10,2),
        order_status STRING,
        payment_method STRING,
        created_time TIMESTAMP,
        etl_timestamp TIMESTAMP
      )
      PARTITIONED BY (dt STRING)
      STORED AS PARQUET
    """)
  }
  
  override protected def loadSourceData(date: String): DataFrame = {
    // 加载订单主表
    val orders = spark.sql(s"""
      SELECT 
        o.id as order_id,
        o.user_id,
        o.total_amount,
        o.discount_amount,
        o.final_amount,
        o.status as order_status,
        o.payment_method,
        o.created_time
      FROM $sourceTable o
      WHERE dt = '$date'
    """)
    
    // 加载订单明细
    val orderItems = spark.sql(s"""
      SELECT 
        oi.order_id,
        oi.product_id,
        oi.quantity,
        oi.unit_price
      FROM ods_order_items oi
      WHERE dt = '$date'
    """)
    
    // 关联订单主表和明细
    orders.join(orderItems, "order_id")
  }
  
  override protected def processDimensions(df: DataFrame): DataFrame = {
    // 加载维度表
    val dimUser = loadDimensionTable("dwd_dim_users")
    val dimProduct = loadDimensionTable("dwd_dim_products")
    val dimDate = loadDimensionTable("dwd_dim_date")
    
    // 关联维度表
    df.join(dimUser, df("user_id") === dimUser("user_id"), "left")
      .join(dimProduct, df("product_id") === dimProduct("product_id"), "left")
      .join(dimDate, to_date(df("created_time")) === dimDate("full_date"), "left")
      .select(
        // 事实表度量
        df("order_id"),
        dimUser("user_key"),
        dimProduct("product_key"),
        dimDate("date_key"),
        df("quantity"),
        df("unit_price"),
        df("total_amount"),
        df("discount_amount"),
        df("final_amount"),
        df("order_status"),
        df("payment_method"),
        df("created_time")
      )
  }
  
  override protected def checkDimensionIntegrity(df: DataFrame): Boolean = {
    // 检查维度关联的完整性
    val totalRows = df.count()
    
    // 检查用户维度关联
    val userNullCount = df.filter(col("user_key").isNull).count()
    val userIntegrityRatio = userNullCount.toDouble / totalRows
    
    // 检查产品维度关联
    val productNullCount = df.filter(col("product_key").isNull).count()
    val productIntegrityRatio = productNullCount.toDouble / totalRows
    
    // 检查日期维度关联
    val dateNullCount = df.filter(col("date_key").isNull).count()
    val dateIntegrityRatio = dateNullCount.toDouble / totalRows
    
    // 允许最多5%的维度关联缺失
    userIntegrityRatio <= 0.05 && 
    productIntegrityRatio <= 0.05 && 
    dateIntegrityRatio <= 0.05
  }
  
  override protected def calculateMetrics(df: DataFrame): DataFrame = {
    df.groupBy("date_key", "product_key")
      .agg(
        count("order_id").alias("order_count"),
        sum("quantity").alias("total_quantity"),
        sum("total_amount").alias("total_sales_amount"),
        sum("discount_amount").alias("total_discount_amount"),
        sum("final_amount").alias("total_final_amount"),
        avg("unit_price").alias("avg_unit_price")
      )
  }
} 