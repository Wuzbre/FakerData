package com.sales.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class InventoryFactProcessor(spark: SparkSession) extends FactProcessor(spark) {
  override protected val factName: String = "inventory"
  override protected val sourceTable: String = "ods_inventory_updates"
  override protected val targetTable: String = "dwd_fact_inventory"
  
  override protected def createFactTable(): Unit = {
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $targetTable (
        inventory_key BIGINT,
        product_key BIGINT,
        date_key INT,
        warehouse_id STRING,
        quantity_change INT,
        current_quantity INT,
        update_type STRING,
        unit_cost DECIMAL(10,2),
        total_cost DECIMAL(10,2),
        created_time TIMESTAMP,
        etl_timestamp TIMESTAMP
      )
      PARTITIONED BY (dt STRING)
      STORED AS PARQUET
    """)
  }
  
  override protected def loadSourceData(date: String): DataFrame = {
    spark.sql(s"""
      SELECT 
        product_id,
        warehouse_id,
        quantity_change,
        current_quantity,
        update_type,
        unit_cost,
        total_cost,
        created_time
      FROM $sourceTable
      WHERE dt = '$date'
    """)
  }
  
  override protected def processDimensions(df: DataFrame): DataFrame = {
    // 加载维度表
    val dimProduct = loadDimensionTable("dwd_dim_products")
    val dimDate = loadDimensionTable("dwd_dim_date")
    
    // 关联维度表
    df.join(dimProduct, df("product_id") === dimProduct("product_id"), "left")
      .join(dimDate, to_date(df("created_time")) === dimDate("full_date"), "left")
      .select(
        // 事实表度量
        dimProduct("product_key"),
        dimDate("date_key"),
        df("warehouse_id"),
        df("quantity_change"),
        df("current_quantity"),
        df("update_type"),
        df("unit_cost"),
        df("total_cost"),
        df("created_time")
      )
  }
  
  override protected def checkDimensionIntegrity(df: DataFrame): Boolean = {
    val totalRows = df.count()
    
    // 检查产品维度关联
    val productNullCount = df.filter(col("product_key").isNull).count()
    val productIntegrityRatio = productNullCount.toDouble / totalRows
    
    // 检查日期维度关联
    val dateNullCount = df.filter(col("date_key").isNull).count()
    val dateIntegrityRatio = dateNullCount.toDouble / totalRows
    
    // 允许最多5%的维度关联缺失
    productIntegrityRatio <= 0.05 && dateIntegrityRatio <= 0.05
  }
  
  override protected def calculateMetrics(df: DataFrame): DataFrame = {
    df.groupBy("date_key", "product_key", "warehouse_id")
      .agg(
        sum("quantity_change").alias("total_quantity_change"),
        last("current_quantity").alias("end_quantity"),
        count("*").alias("update_count"),
        sum(when(col("update_type") === "IN", col("quantity_change")).otherwise(0)).alias("total_in_quantity"),
        sum(when(col("update_type") === "OUT", col("quantity_change")).otherwise(0)).alias("total_out_quantity"),
        avg("unit_cost").alias("avg_unit_cost"),
        sum("total_cost").alias("total_cost")
      )
  }
  
  // 计算库存周转率
  def calculateInventoryTurnover(startDate: String, endDate: String): DataFrame = {
    spark.sql(s"""
      WITH inventory_daily AS (
        SELECT 
          product_key,
          date_key,
          warehouse_id,
          current_quantity as daily_inventory
        FROM $targetTable
        WHERE dt BETWEEN '$startDate' AND '$endDate'
      ),
      avg_inventory AS (
        SELECT 
          product_key,
          warehouse_id,
          avg(daily_inventory) as average_inventory
        FROM inventory_daily
        GROUP BY product_key, warehouse_id
      ),
      total_sales AS (
        SELECT 
          product_key,
          sum(quantity) as total_quantity_sold
        FROM dwd_fact_sales
        WHERE dt BETWEEN '$startDate' AND '$endDate'
        GROUP BY product_key
      )
      SELECT 
        i.product_key,
        i.warehouse_id,
        i.average_inventory,
        s.total_quantity_sold,
        s.total_quantity_sold / i.average_inventory as turnover_rate
      FROM avg_inventory i
      JOIN total_sales s ON i.product_key = s.product_key
      WHERE i.average_inventory > 0
    """)
  }
  
  // 计算安全库存水平
  def calculateSafetyStock(windowDays: Int): DataFrame = {
    spark.sql(s"""
      WITH daily_demand AS (
        SELECT 
          product_key,
          date_key,
          sum(quantity) as daily_demand
        FROM dwd_fact_sales
        WHERE dt >= date_sub(current_date, $windowDays)
        GROUP BY product_key, date_key
      ),
      demand_stats AS (
        SELECT 
          product_key,
          avg(daily_demand) as avg_daily_demand,
          stddev(daily_demand) as stddev_daily_demand
        FROM daily_demand
        GROUP BY product_key
      )
      SELECT 
        product_key,
        avg_daily_demand,
        stddev_daily_demand,
        -- 假设服务水平为95%（对应Z值为1.96）
        avg_daily_demand * 7 + (1.96 * stddev_daily_demand * sqrt(7)) as safety_stock_level
      FROM demand_stats
    """)
  }
} 