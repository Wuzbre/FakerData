package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 用户消费趋势分析处理类
 */
class UserConsumptionTrendProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_user_consumption_trend_m"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的用户行为数据（按月汇总）
    val userBehavior = spark.sql(
      s"""
         |SELECT 
         |  user_id,
         |  user_level,
         |  total_amount,
         |  substr(dt, 1, 7) as dt
         |FROM dws.dws_user_behavior
         |WHERE substr(dt, 1, 7) = substr('$dt', 1, 7)
         |""".stripMargin)
    
    // 2. 按月份和用户分组汇总
    val monthlyConsumption = userBehavior
      .groupBy("dt", "user_id", "user_level")
      .agg(sum("total_amount").as("total_amount"))
    
    Map("monthly_consumption" -> monthlyConsumption)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val monthlyConsumption = data("monthly_consumption")
    
    // 1. 计算环比增长率
    val withMoM = calculateMoM(monthlyConsumption, "total_amount")
    
    // 2. 计算同比增长率
    val withYoY = calculateYoY(withMoM, "total_amount")
    
    // 3. 四舍五入数值字段
    withYoY.select(
      col("dt"),
      col("user_id"),
      col("user_level"),
      round(col("total_amount"), 2).as("total_amount"),
      round(col("mom_rate"), 4).as("mom_rate"),
      round(col("yoy_rate"), 4).as("yoy_rate")
    )
  }
} 