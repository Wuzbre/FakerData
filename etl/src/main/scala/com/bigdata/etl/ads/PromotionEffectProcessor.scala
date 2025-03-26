package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 促销活动效果排行处理类
 */
class PromotionEffectProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_promotion_effect_rank_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的促销活动效果数据
    val promotionEffect = spark.sql(
      s"""
         |SELECT 
         |  p.promotion_id,
         |  p.promotion_name,
         |  p.promotion_type,
         |  p.total_sales_amount,
         |  p.total_discount_amount,
         |  p.customer_count,
         |  p.promotion_cost,
         |  p.dt
         |FROM dws.dws_promotion_effect p
         |WHERE p.dt = '$dt'
         |""".stripMargin)
    
    Map("promotion_effect" -> promotionEffect)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val promotionEffect = data("promotion_effect")
    
    // 1. 计算ROI和效果得分
    val withMetrics = promotionEffect
      // 计算投资回报率 ROI = (销售额 - 成本) / 成本
      .withColumn("promotion_roi",
        when(col("promotion_cost") > 0,
          (col("total_sales_amount") - col("promotion_cost")) / col("promotion_cost")
        ).otherwise(0.0)
      )
    
    // 2. 计算效果得分
    // 得分 = 标准化销售额 * 0.3 + 标准化ROI * 0.3 + 标准化客户数 * 0.2 + 标准化折扣率 * 0.2
    val withNormalized = withMetrics
      .withColumn("norm_sales",
        (col("total_sales_amount") - min("total_sales_amount").over()) /
        (max("total_sales_amount").over() - min("total_sales_amount").over())
      )
      .withColumn("norm_roi",
        (col("promotion_roi") - min("promotion_roi").over()) /
        (max("promotion_roi").over() - min("promotion_roi").over())
      )
      .withColumn("norm_customers",
        (col("customer_count") - min("customer_count").over()) /
        (max("customer_count").over() - min("customer_count").over())
      )
      .withColumn("discount_rate",
        when(col("total_sales_amount") > 0,
          col("total_discount_amount") / (col("total_sales_amount") + col("total_discount_amount"))
        ).otherwise(0.0)
      )
      .withColumn("norm_discount",
        (col("discount_rate") - min("discount_rate").over()) /
        (max("discount_rate").over() - min("discount_rate").over())
      )
      .withColumn("effectiveness_score",
        col("norm_sales") * 0.3 +
        col("norm_roi") * 0.3 +
        col("norm_customers") * 0.2 +
        col("norm_discount") * 0.2
      )
    
    // 3. 计算排名
    val withRank = calculateRank(withNormalized, "effectiveness_score")
    
    // 4. 四舍五入数值字段
    withRank.select(
      col("dt"),
      col("promotion_id"),
      col("promotion_name"),
      col("promotion_type"),
      round(col("total_sales_amount"), 2).as("total_sales_amount"),
      round(col("total_discount_amount"), 2).as("total_discount_amount"),
      col("customer_count"),
      round(col("promotion_roi"), 4).as("promotion_roi"),
      round(col("effectiveness_score"), 2).as("effectiveness_score"),
      col("rank_num")
    )
  }
} 