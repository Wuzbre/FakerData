package com.bigdata.etl.summary

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 促销效果汇总处理类
 */
class PromotionEffectSummary(spark: SparkSession) extends SummaryProcessor(spark) {
  import spark.implicits._

  /**
   * 处理促销效果数据
   */
  private def processPromotionEffect(): DataFrame = {
    // 读取促销事实表
    val promotionsDf = readFromDWD("dwd_fact_promotion")
    
    // 读取订单事实表
    val ordersDf = readFromDWD("dwd_fact_order")
      .select("order_id", "user_id", "promotion_id", "order_status", "dt")
      .where(col("order_status") === 3) // 只统计已完成订单
    
    // 读取订单明细事实表
    val orderDetailsDf = readFromDWD("dwd_fact_order_detail")
      .select("order_id", "product_id", "quantity", "total_price", "discount_amount", "dt")
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select("product_id", "category_id", "category_name")
    
    // 统计促销活动基础指标
    val promotionBasicStatsDf = ordersDf
      .join(orderDetailsDf, Seq("order_id", "dt"))
      .where(col("promotion_id").isNotNull)
      .groupBy("promotion_id", "dt")
      .agg(
        countDistinct("order_id").as("order_count"),
        countDistinct("user_id").as("customer_count"),
        countDistinct("product_id").as("affected_product_count"),
        sum("total_price").as("total_sales_amount"),
        sum("discount_amount").as("total_discount_amount")
      )
    
    // 计算促销活动的商品类目影响
    val categoryEffectDf = ordersDf
      .join(orderDetailsDf, Seq("order_id", "dt"))
      .join(productDf, Seq("product_id"))
      .where(col("promotion_id").isNotNull)
      .groupBy("promotion_id", "dt")
      .agg(
        countDistinct("category_id").as("affected_category_count"),
        collect_set("category_name").as("affected_categories")
      )
    
    // 计算促销活动的客户行为指标
    val customerBehaviorDf = ordersDf
      .join(orderDetailsDf, Seq("order_id", "dt"))
      .where(col("promotion_id").isNotNull)
      .groupBy("promotion_id", "user_id", "dt")
      .agg(
        count("order_id").as("user_order_count"),
        sum("total_price").as("user_total_amount")
      )
      .groupBy("promotion_id", "dt")
      .agg(
        avg("user_order_count").as("avg_user_order_count"),
        avg("user_total_amount").as("avg_user_amount"),
        count(when(col("user_order_count") > 1, 1)).as("repeat_customer_count")
      )
    
    // 计算促销效果指标
    val promotionEffectDf = promotionBasicStatsDf
      .join(categoryEffectDf, Seq("promotion_id", "dt"))
      .join(customerBehaviorDf, Seq("promotion_id", "dt"))
      .join(promotionsDf, Seq("promotion_id", "dt"))
      .withColumn("promotion_roi", 
        when(col("total_discount_amount") > 0,
          (col("total_sales_amount") - col("total_discount_amount")) / col("total_discount_amount"))
        .otherwise(0.0))
      .withColumn("customer_retention_rate",
        when(col("customer_count") > 0,
          col("repeat_customer_count") / col("customer_count"))
        .otherwise(0.0))
      .withColumn("avg_discount_rate",
        when(col("total_sales_amount") > 0,
          col("total_discount_amount") / col("total_sales_amount"))
        .otherwise(0.0))
    
    // 计算促销活动排名
    val windowSpec = Window.partitionBy("dt").orderBy(desc("promotion_roi"))
    val promotionRankDf = promotionEffectDf
      .withColumn("roi_rank", dense_rank().over(windowSpec))
      .withColumn("effectiveness_score",
        (col("promotion_roi") * 0.4 +
         col("customer_retention_rate") * 100 * 0.3 +
         (lit(1) - col("avg_discount_rate")) * 100 * 0.3))
    
    // 计算同环比
    val promotionMetricsWithGrowthDf = calculateGrowthRate(
      promotionRankDf,
      Seq("promotion_id"),
      "total_sales_amount"
    )
    
    // 添加ETL时间
    addETLColumns(promotionMetricsWithGrowthDf)
  }

  /**
   * 处理促销效果汇总数据
   */
  override def process(): Unit = {
    try {
      // 处理促销效果数据
      val promotionEffectDf = processPromotionEffect()
      
      // 保存到Hive表
      saveToHive(promotionEffectDf, "dws.dws_promotion_effect")
      println("促销效果汇总表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"促销效果汇总表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 