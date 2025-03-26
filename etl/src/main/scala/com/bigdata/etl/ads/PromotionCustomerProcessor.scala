package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 促销活动客户分析处理类
 */
class PromotionCustomerProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_promotion_customer_stats_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的促销活动客户数据
    val promotionCustomer = spark.sql(
      s"""
         |SELECT 
         |  p.promotion_id,
         |  p.promotion_name,
         |  p.customer_count,
         |  p.total_orders,
         |  p.total_amount,
         |  p.repeat_purchase_count,
         |  p.dt
         |FROM dws.dws_promotion_customer p
         |WHERE p.dt = '$dt'
         |""".stripMargin)
    
    Map("promotion_customer" -> promotionCustomer)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val promotionCustomer = data("promotion_customer")
    
    // 1. 计算客户指标
    val withMetrics = promotionCustomer
      // 计算平均订单数
      .withColumn("avg_user_orders",
        when(col("customer_count") > 0,
          col("total_orders") / col("customer_count")
        ).otherwise(0.0)
      )
      // 计算平均消费金额
      .withColumn("avg_user_amount",
        when(col("customer_count") > 0,
          col("total_amount") / col("customer_count")
        ).otherwise(0.0)
      )
      // 计算客户留存率
      .withColumn("retention_rate",
        when(col("customer_count") > 0,
          col("repeat_purchase_count") / col("customer_count")
        ).otherwise(0.0)
      )
    
    // 2. 四舍五入数值字段
    withMetrics.select(
      col("dt"),
      col("promotion_id"),
      col("promotion_name"),
      col("customer_count"),
      round(col("avg_user_orders"), 2).as("avg_user_orders"),
      round(col("avg_user_amount"), 2).as("avg_user_amount"),
      round(col("retention_rate"), 4).as("retention_rate")
    )
  }
} 