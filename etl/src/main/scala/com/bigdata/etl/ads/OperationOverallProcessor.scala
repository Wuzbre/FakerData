package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 整体运营指标处理类
 */
class OperationOverallProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_operation_overall_stats_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取用户数据
    val userData = spark.sql(
      s"""
         |SELECT 
         |  count(distinct user_id) as total_users,
         |  sum(total_amount) as total_sales,
         |  avg(total_amount) as avg_customer_value,
         |  dt
         |FROM dws.dws_user_behavior
         |WHERE dt = '$dt'
         |GROUP BY dt
         |""".stripMargin)
    
    // 2. 读取订单数据
    val orderData = spark.sql(
      s"""
         |SELECT 
         |  count(case when is_promotion = 1 then order_id end) as promo_orders,
         |  count(order_id) as total_orders,
         |  dt
         |FROM dws.dws_order
         |WHERE dt = '$dt'
         |GROUP BY dt
         |""".stripMargin)
    
    // 3. 读取库存数据
    val inventoryData = spark.sql(
      s"""
         |SELECT 
         |  count(case when stock_status = '正常' then 1 end) as normal_stock,
         |  count(*) as total_products,
         |  avg(stock_health_score) as avg_inventory_score,
         |  dt
         |FROM dws.dws_inventory
         |WHERE dt = '$dt'
         |GROUP BY dt
         |""".stripMargin)
    
    // 4. 读取供应商数据
    val supplierData = spark.sql(
      s"""
         |SELECT 
         |  avg(supplier_score) as avg_supplier_score,
         |  dt
         |FROM dws.dws_supplier_performance
         |WHERE dt = '$dt'
         |GROUP BY dt
         |""".stripMargin)
    
    Map(
      "user" -> userData,
      "order" -> orderData,
      "inventory" -> inventoryData,
      "supplier" -> supplierData
    )
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val userData = data("user")
    val orderData = data("order")
    val inventoryData = data("inventory")
    val supplierData = data("supplier")
    
    // 1. 合并各个维度的数据
    val result = userData
      .join(orderData, "dt")
      .join(inventoryData, "dt")
      .join(supplierData, "dt")
    
    // 2. 计算比率指标
    val withRatios = result
      // 计算促销订单占比
      .withColumn("promotion_order_ratio",
        when(col("total_orders") > 0,
          col("promo_orders") / col("total_orders")
        ).otherwise(0.0)
      )
      // 计算正常库存商品占比
      .withColumn("normal_stock_ratio",
        when(col("total_products") > 0,
          col("normal_stock") / col("total_products")
        ).otherwise(0.0)
      )
    
    // 3. 四舍五入数值字段
    withRatios.select(
      col("dt"),
      col("total_users"),
      round(col("total_sales"), 2).as("total_sales"),
      round(col("avg_customer_value"), 2).as("avg_customer_value"),
      round(col("promotion_order_ratio"), 4).as("promotion_order_ratio"),
      round(col("normal_stock_ratio"), 4).as("normal_stock_ratio"),
      round(col("avg_inventory_score"), 2).as("avg_inventory_score"),
      round(col("avg_supplier_score"), 2).as("avg_supplier_score")
    )
  }
} 