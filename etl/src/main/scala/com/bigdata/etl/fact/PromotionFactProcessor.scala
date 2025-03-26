package com.bigdata.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 促销事实表处理类
 */
class PromotionFactProcessor(spark: SparkSession) extends FactProcessor(spark) {
  import spark.implicits._

  /**
   * 处理促销活动数据
   */
  private def processPromotions(): DataFrame = {
    // 读取促销活动表
    val promotionsDf = readFromODS("ods_promotions")
    
    // 读取订单明细表
    val orderDetailsDf = readFromODS("ods_order_details")
      .where(col("is_promotion_price") === true)
    
    // 统计促销参与情况
    val promotionStatsDf = orderDetailsDf
      .groupBy("promotion_id")
      .agg(
        countDistinct("order_id").as("order_count"),
        countDistinct("user_id").as("participant_count"),
        sum("discount_amount").as("total_discount_amount"),
        sum("total_price").as("total_sales_amount")
      )
    
    // 关联促销活动信息
    val promotionWithStatsDf = promotionsDf
      .join(promotionStatsDf, Seq("promotion_id"), "left")
      .na.fill(0, Seq("order_count", "participant_count", "total_discount_amount", "total_sales_amount"))
    
    // 计算促销ROI
    val promotionWithROIDf = promotionWithStatsDf
      .withColumn("promotion_roi", 
        when(col("total_discount_amount") > 0,
          col("total_sales_amount") / col("total_discount_amount"))
        .otherwise(lit(0.0)))
    
    // 添加ETL时间和分区字段
    addETLColumns(promotionWithROIDf)
  }

  /**
   * 处理促销相关的所有事实表数据
   */
  override def process(): Unit = {
    try {
      // 处理促销活动表
      val promotionsDf = processPromotions()
      saveToHive(promotionsDf, "dwd.dwd_fact_promotion")
      println("促销事实表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"促销事实表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 