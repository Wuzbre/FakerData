package com.bigdata.etl.summary

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 商品销售汇总处理类
 */
class ProductSalesSummary(spark: SparkSession) extends SummaryProcessor(spark) {
  import spark.implicits._

  /**
   * 处理商品销售数据
   */
  private def processProductSales(): DataFrame = {
    // 读取订单明细事实表
    val orderDetailsDf = readFromDWD("dwd_fact_order_detail")
    
    // 读取订单事实表（用于关联订单状态）
    val ordersDf = readFromDWD("dwd_fact_order")
      .select("order_id", "order_status", "dt")
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select("product_id", "product_name", "category_id", "category_name", "supplier_id")
    
    // 关联订单状态，只统计已完成的订单
    val validOrderDetailsDf = orderDetailsDf
      .join(ordersDf, Seq("order_id", "dt"))
      .where(col("order_status") === 3) // 假设3表示已完成状态
    
    // 统计商品销售基础指标
    val productBasicStatsDf = validOrderDetailsDf
      .groupBy("product_id", "dt")
      .agg(
        count("order_id").as("order_count"),
        sum("quantity").as("total_quantity"),
        sum("total_price").as("total_amount"),
        avg("unit_price").as("avg_price"),
        avg("rating").as("avg_rating"),
        count("rating").as("rating_count")
      )
    
    // 统计促销相关指标
    val productPromotionStatsDf = validOrderDetailsDf
      .where(col("is_promotion_price") === true)
      .groupBy("product_id", "dt")
      .agg(
        sum("quantity").as("promotion_quantity"),
        sum("total_price").as("promotion_amount"),
        sum("discount_amount").as("total_discount")
      )
    
    // 计算商品热度分
    val windowSpec = Window.partitionBy("dt")
    val productPopularityDf = productBasicStatsDf
      .withColumn("sales_rank", 
        dense_rank().over(windowSpec.orderBy(desc("total_amount"))))
      .withColumn("quantity_rank",
        dense_rank().over(windowSpec.orderBy(desc("total_quantity"))))
      .withColumn("popularity_score",
        (col("total_quantity") * 0.4 + 
         col("order_count") * 0.3 +
         col("rating_count") * 0.2 +
         col("avg_rating") * 0.1))
    
    // 关联所有统计结果
    val productSalesDf = productPopularityDf
      .join(productPromotionStatsDf, Seq("product_id", "dt"), "left")
      .join(productDf, Seq("product_id"))
      .na.fill(0) // 填充空值
    
    // 计算促销效果指标
    val productMetricsDf = productSalesDf
      .withColumn("promotion_rate",
        when(col("total_quantity") > 0,
          col("promotion_quantity") / col("total_quantity"))
        .otherwise(0.0))
      .withColumn("discount_rate",
        when(col("total_amount") > 0,
          col("total_discount") / col("total_amount"))
        .otherwise(0.0))
    
    // 计算同环比
    val productMetricsWithGrowthDf = calculateGrowthRate(
      productMetricsDf,
      Seq("product_id"),
      "total_amount"
    )
    
    // 添加ETL时间
    addETLColumns(productMetricsWithGrowthDf)
  }

  /**
   * 处理商品销售汇总数据
   */
  override def process(): Unit = {
    try {
      // 处理商品销售数据
      val productSalesDf = processProductSales()
      
      // 保存到Hive表
      saveToHive(productSalesDf, "dws.dws_product_sales")
      println("商品销售汇总表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"商品销售汇总表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 