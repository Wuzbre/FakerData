package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 供应商评分处理类
 */
class SupplierScoreProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_supplier_score_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的供应商表现数据
    val supplierPerformance = spark.sql(
      s"""
         |SELECT 
         |  s.supplier_id,
         |  s.supplier_name,
         |  s.total_sales_amount,
         |  s.delivery_orders,
         |  s.on_time_deliveries,
         |  s.quality_issue_orders,
         |  s.dt
         |FROM dws.dws_supplier_performance s
         |WHERE s.dt = '$dt'
         |""".stripMargin)
    
    Map("supplier_performance" -> supplierPerformance)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val supplierPerformance = data("supplier_performance")
    
    // 1. 计算关键指标
    val withRates = supplierPerformance
      .withColumn("on_time_delivery_rate",
        when(col("delivery_orders") > 0,
          col("on_time_deliveries") / col("delivery_orders")
        ).otherwise(0.0)
      )
      .withColumn("quality_issue_rate",
        when(col("delivery_orders") > 0,
          col("quality_issue_orders") / col("delivery_orders")
        ).otherwise(0.0)
      )
    
    // 2. 计算供应商得分
    // 得分 = 标准化销售额 * 0.4 + (1 - 标准化质量问题率) * 0.3 + 标准化准时率 * 0.3
    val withNormalized = withRates
      .withColumn("norm_sales",
        (col("total_sales_amount") - min("total_sales_amount").over()) /
        (max("total_sales_amount").over() - min("total_sales_amount").over())
      )
      .withColumn("norm_quality",
        (col("quality_issue_rate") - min("quality_issue_rate").over()) /
        (max("quality_issue_rate").over() - min("quality_issue_rate").over())
      )
      .withColumn("norm_delivery",
        (col("on_time_delivery_rate") - min("on_time_delivery_rate").over()) /
        (max("on_time_delivery_rate").over() - min("on_time_delivery_rate").over())
      )
      .withColumn("supplier_score",
        col("norm_sales") * 0.4 +
        (lit(1.0) - col("norm_quality")) * 0.3 +
        col("norm_delivery") * 0.3
      )
    
    // 3. 计算排名
    val withRank = calculateRank(withNormalized, "supplier_score")
    
    // 4. 四舍五入数值字段
    withRank.select(
      col("dt"),
      col("supplier_id"),
      col("supplier_name"),
      round(col("total_sales_amount"), 2).as("total_sales_amount"),
      round(col("on_time_delivery_rate"), 4).as("on_time_delivery_rate"),
      round(col("quality_issue_rate"), 4).as("quality_issue_rate"),
      round(col("supplier_score"), 2).as("supplier_score"),
      col("rank_num")
    )
  }
} 