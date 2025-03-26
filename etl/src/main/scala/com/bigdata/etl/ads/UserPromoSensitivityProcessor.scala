package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 用户促销敏感度分析处理类
 */
class UserPromoSensitivityProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_user_promo_sensitivity_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的用户行为数据
    val userBehavior = spark.sql(
      s"""
         |SELECT 
         |  user_id,
         |  user_level,
         |  total_amount,
         |  promo_order_count,
         |  total_discount_amount,
         |  dt
         |FROM dws.dws_user_behavior
         |WHERE dt = '$dt'
         |""".stripMargin)
    
    Map("user_behavior" -> userBehavior)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val userBehavior = data("user_behavior")
    
    // 1. 计算促销敏感度
    val withSensitivity = userBehavior
      .withColumn("promo_sensitivity",
        when(col("total_amount") > 0,
          col("total_discount_amount") / col("total_amount")
        ).otherwise(0.0)
      )
    
    // 2. 按用户等级分组统计
    val result = withSensitivity.groupBy("dt", "user_level")
      .agg(
        avg("promo_sensitivity").as("avg_promo_sensitivity"),
        avg("promo_order_count").as("avg_promo_orders"),
        avg("total_discount_amount").as("avg_discount_amount")
      )
    
    // 3. 四舍五入数值字段
    result.select(
      col("dt"),
      col("user_level"),
      round(col("avg_promo_sensitivity"), 4).as("avg_promo_sensitivity"),
      round(col("avg_promo_orders"), 2).as("avg_promo_orders"),
      round(col("avg_discount_amount"), 2).as("avg_discount_amount")
    )
  }
} 