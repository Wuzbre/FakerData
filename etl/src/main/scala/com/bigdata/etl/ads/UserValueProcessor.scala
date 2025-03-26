package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 用户价值分析处理类
 */
class UserValueProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_user_value_stats_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的用户行为汇总数据
    val userBehavior = spark.sql(
      s"""
         |SELECT 
         |  user_id,
         |  user_level,
         |  total_amount,
         |  order_count,
         |  dt
         |FROM dws.dws_user_behavior
         |WHERE dt = '$dt'
         |""".stripMargin)
    
    Map("user_behavior" -> userBehavior)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val userBehavior = data("user_behavior")
    
    // 1. 按用户等级分组统计
    val result = userBehavior.groupBy("dt", "user_level")
      .agg(
        count("user_id").as("user_count"),
        avg("total_amount").as("avg_customer_value"),
        avg("total_amount").as("avg_total_amount"),
        avg("order_count").as("avg_order_count")
      )
    
    // 2. 四舍五入金额字段到2位小数
    result.select(
      col("dt"),
      col("user_level"),
      col("user_count"),
      round(col("avg_customer_value"), 2).as("avg_customer_value"),
      round(col("avg_total_amount"), 2).as("avg_total_amount"),
      round(col("avg_order_count"), 2).as("avg_order_count")
    )
  }
} 