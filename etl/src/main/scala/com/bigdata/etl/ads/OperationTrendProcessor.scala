package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 运营趋势分析处理类
 */
class OperationTrendProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_operation_trend_m"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取月度运营数据
    val monthlyData = spark.sql(
      s"""
         |SELECT 
         |  substr(dt, 1, 7) as dt,
         |  count(distinct user_id) as total_users,
         |  count(distinct order_id) as total_orders,
         |  sum(total_amount) as total_sales,
         |  avg(order_amount) as avg_order_amount
         |FROM dws.dws_order
         |WHERE substr(dt, 1, 7) <= substr('$dt', 1, 7)
         |GROUP BY substr(dt, 1, 7)
         |""".stripMargin)
    
    Map("monthly" -> monthlyData)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val monthlyData = data("monthly")
    
    // 1. 计算环比增长率
    val withMoM = calculateMoM(monthlyData, "total_sales")
      .withColumnRenamed("mom_rate", "sales_mom_rate")
    
    val withUserMoM = calculateMoM(withMoM, "total_users")
      .withColumnRenamed("mom_rate", "user_mom_rate")
    
    // 2. 计算同比增长率
    val withYoY = calculateYoY(withUserMoM, "total_sales")
      .withColumnRenamed("yoy_rate", "sales_yoy_rate")
    
    val withUserYoY = calculateYoY(withYoY, "total_users")
      .withColumnRenamed("yoy_rate", "user_yoy_rate")
    
    // 3. 四舍五入数值字段
    withUserYoY.select(
      col("dt"),
      round(col("total_sales"), 2).as("total_sales"),
      col("total_users"),
      col("total_orders"),
      round(col("avg_order_amount"), 2).as("avg_order_amount"),
      round(col("sales_mom_rate"), 4).as("sales_mom_rate"),
      round(col("sales_yoy_rate"), 4).as("sales_yoy_rate"),
      round(col("user_mom_rate"), 4).as("user_mom_rate"),
      round(col("user_yoy_rate"), 4).as("user_yoy_rate")
    )
  }
} 