package com.bigdata.etl.summary

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 供应商表现汇总处理类
 */
class SupplierPerformanceSummary(spark: SparkSession) extends SummaryProcessor(spark) {
  import spark.implicits._

  /**
   * 处理供应商销售表现数据
   */
  private def processSupplierSales(): DataFrame = {
    // 读取订单明细事实表
    val orderDetailsDf = readFromDWD("dwd_fact_order_detail")
    
    // 读取订单事实表
    val ordersDf = readFromDWD("dwd_fact_order")
      .select("order_id", "order_status", "delivery_days", "dt")
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select("product_id", "supplier_id", "category_id")
    
    // 读取供应商维度表
    val supplierDf = readFromDIM("dim_supplier")
      .select("supplier_id", "supplier_name", "supply_capacity", "credit_score")
    
    // 关联订单状态，只统计已完成的订单
    val validOrderDetailsDf = orderDetailsDf
      .join(ordersDf, Seq("order_id", "dt"))
      .where(col("order_status") === 3) // 假设3表示已完成状态
      .join(productDf, Seq("product_id"))
    
    // 统计供应商销售基础指标
    val supplierBasicStatsDf = validOrderDetailsDf
      .groupBy("supplier_id", "dt")
      .agg(
        countDistinct("product_id").as("product_count"),
        sum("quantity").as("total_supply_quantity"),
        sum("total_price").as("total_sales_amount"),
        avg("delivery_days").as("avg_delivery_days"),
        avg("rating").as("avg_rating")
      )
    
    // 统计供应商的准时交付率
    val deliveryStatsDf = validOrderDetailsDf
      .withColumn("is_on_time", col("delivery_days") <= 3) // 假设3天为准时标准
      .groupBy("supplier_id", "dt")
      .agg(
        count(when(col("is_on_time") === true, 1)).as("on_time_deliveries"),
        count("*").as("total_deliveries"),
        count(when(col("rating") < 3, 1)).as("low_rating_count") // 假设评分小于3为质量问题
      )
      .withColumn("on_time_delivery_rate",
        when(col("total_deliveries") > 0,
          col("on_time_deliveries") / col("total_deliveries"))
        .otherwise(0.0))
      .withColumn("quality_issue_rate",
        when(col("total_deliveries") > 0,
          col("low_rating_count") / col("total_deliveries"))
        .otherwise(0.0))
    
    // 统计供应商的品类覆盖
    val categoryCoverageDf = validOrderDetailsDf
      .groupBy("supplier_id", "dt")
      .agg(
        countDistinct("category_id").as("category_count"),
        count(when(col("is_promotion_price") === true, 1)).as("promotion_order_count")
      )
    
    // 计算供应商的综合得分
    val windowSpec = Window.partitionBy("dt")
    val supplierScoreDf = supplierBasicStatsDf
      .join(deliveryStatsDf, Seq("supplier_id", "dt"))
      .join(categoryCoverageDf, Seq("supplier_id", "dt"))
      .withColumn("sales_rank",
        dense_rank().over(windowSpec.orderBy(desc("total_sales_amount"))))
      .withColumn("supplier_score",
        (col("total_sales_amount") * 0.3 +
         col("on_time_delivery_rate") * 100 * 0.3 +
         (lit(1) - col("quality_issue_rate")) * 100 * 0.2 +
         col("avg_rating") * 20 * 0.2))
    
    // 关联供应商基础信息
    val supplierPerformanceDf = supplierScoreDf
      .join(supplierDf, Seq("supplier_id"))
    
    // 计算同环比
    val supplierMetricsWithGrowthDf = calculateGrowthRate(
      supplierPerformanceDf,
      Seq("supplier_id"),
      "total_sales_amount"
    )
    
    // 添加ETL时间
    addETLColumns(supplierMetricsWithGrowthDf)
  }

  /**
   * 处理供应商表现汇总数据
   */
  override def process(): Unit = {
    try {
      // 处理供应商表现数据
      val supplierPerformanceDf = processSupplierSales()
      
      // 保存到Hive表
      saveToHive(supplierPerformanceDf, "dws.dws_supplier_performance")
      println("供应商表现汇总表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"供应商表现汇总表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 