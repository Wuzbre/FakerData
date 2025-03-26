package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 供应商交付分析处理类
 */
class SupplierDeliveryProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_supplier_delivery_stats_m"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的供应商表现数据（按月汇总）
    val supplierPerformance = spark.sql(
      s"""
         |SELECT 
         |  s.supplier_id,
         |  s.supplier_name,
         |  s.delivery_orders,
         |  s.on_time_deliveries,
         |  s.quality_issue_orders,
         |  s.avg_delivery_days,
         |  substr(s.dt, 1, 7) as dt
         |FROM dws.dws_supplier_performance s
         |WHERE substr(s.dt, 1, 7) = substr('$dt', 1, 7)
         |""".stripMargin)
    
    // 2. 按月份和供应商分组汇总
    val monthlyDelivery = supplierPerformance
      .groupBy("dt", "supplier_id", "supplier_name")
      .agg(
        sum("delivery_orders").as("total_deliveries"),
        sum("on_time_deliveries").as("total_on_time"),
        sum("quality_issue_orders").as("total_quality_issues"),
        avg("avg_delivery_days").as("avg_delivery_days")
      )
    
    Map("monthly_delivery" -> monthlyDelivery)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val monthlyDelivery = data("monthly_delivery")
    
    // 1. 计算关键指标
    val withRates = monthlyDelivery
      .withColumn("on_time_delivery_rate",
        when(col("total_deliveries") > 0,
          col("total_on_time") / col("total_deliveries")
        ).otherwise(0.0)
      )
      .withColumn("quality_issue_rate",
        when(col("total_deliveries") > 0,
          col("total_quality_issues") / col("total_deliveries")
        ).otherwise(0.0)
      )
    
    // 2. 计算环比增长率
    val withMoM = calculateMoM(withRates, "total_deliveries")
    
    // 3. 四舍五入数值字段
    withMoM.select(
      col("dt"),
      col("supplier_id"),
      col("supplier_name"),
      round(col("avg_delivery_days"), 2).as("avg_delivery_days"),
      round(col("on_time_delivery_rate"), 4).as("on_time_delivery_rate"),
      round(col("quality_issue_rate"), 4).as("quality_issue_rate"),
      col("total_deliveries"),
      round(col("mom_rate"), 4).as("mom_rate")
    )
  }
} 