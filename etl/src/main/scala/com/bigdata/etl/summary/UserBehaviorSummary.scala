package com.bigdata.etl.summary

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 用户行为汇总处理类
 */
class UserBehaviorSummary(spark: SparkSession) extends SummaryProcessor(spark) {
  import spark.implicits._

  /**
   * 处理用户订单行为数据
   */
  private def processUserOrderBehavior(): DataFrame = {
    // 读取订单事实表
    val ordersDf = readFromDWD("dwd_fact_order")
    
    // 读取订单明细事实表
    val orderDetailsDf = readFromDWD("dwd_fact_order_detail")
    
    // 读取用户维度表
    val userDf = readFromDIM("dim_user")
      .select("user_id", "user_level")
    
    // 统计用户订单行为
    val userOrderStatsDf = ordersDf
      .groupBy("user_id", "dt")
      .agg(
        count("order_id").as("order_count"),
        sum("total_amount").as("total_amount"),
        sum("actual_amount").as("actual_amount"),
        sum(when(col("is_first_order") === true, 1).otherwise(0)).as("first_order_count"),
        avg("total_amount").as("avg_order_amount")
      )
    
    // 统计用户促销订单
    val userPromotionStatsDf = ordersDf
      .where(col("promotion_id").isNotNull)
      .groupBy("user_id", "dt")
      .agg(
        count("order_id").as("promotion_order_count"),
        sum("promotion_discount").as("total_promotion_discount")
      )
    
    // 统计用户购买商品种类
    val userProductStatsDf = orderDetailsDf
      .groupBy("user_id", "dt")
      .agg(
        countDistinct("product_id").as("purchased_product_count"),
        sum("quantity").as("total_quantity"),
        avg("rating").as("avg_rating")
      )
    
    // 关联所有统计结果
    val userBehaviorDf = userOrderStatsDf
      .join(userPromotionStatsDf, Seq("user_id", "dt"), "left")
      .join(userProductStatsDf, Seq("user_id", "dt"), "left")
      .join(userDf, Seq("user_id"), "left")
      .na.fill(0) // 填充空值
    
    // 计算用户消费能力指标
    val userMetricsDf = userBehaviorDf
      .withColumn("promotion_sensitivity", 
        when(col("order_count") > 0,
          col("promotion_order_count") / col("order_count"))
        .otherwise(0.0))
      .withColumn("discount_sensitivity",
        when(col("total_amount") > 0,
          col("total_promotion_discount") / col("total_amount"))
        .otherwise(0.0))
      .withColumn("customer_value",
        (col("total_amount") * 0.4 + 
         col("order_count") * 0.3 +
         col("purchased_product_count") * 0.2 +
         col("avg_rating") * 0.1))
    
    // 计算同环比
    val userMetricsWithGrowthDf = calculateGrowthRate(
      userMetricsDf,
      Seq("user_id"),
      "total_amount"
    )
    
    // 添加ETL时间
    addETLColumns(userMetricsWithGrowthDf)
  }

  /**
   * 处理用户行为汇总数据
   */
  override def process(): Unit = {
    try {
      // 处理用户行为数据
      val userBehaviorDf = processUserOrderBehavior()
      
      // 保存到Hive表
      saveToHive(userBehaviorDf, "dws.dws_user_behavior")
      println("用户行为汇总表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"用户行为汇总表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 