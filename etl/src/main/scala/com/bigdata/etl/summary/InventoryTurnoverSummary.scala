package com.bigdata.etl.summary

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 库存周转汇总处理类
 */
class InventoryTurnoverSummary(spark: SparkSession) extends SummaryProcessor(spark) {
  import spark.implicits._

  /**
   * 处理库存周转数据
   */
  private def processInventoryTurnover(): DataFrame = {
    // 读取库存变动事实表
    val inventoryChangesDf = readFromDWD("dwd_fact_inventory_change")
    
    // 读取订单明细事实表（用于计算销量）
    val orderDetailsDf = readFromDWD("dwd_fact_order_detail")
      .join(readFromDWD("dwd_fact_order")
        .select("order_id", "order_status", "dt")
        .where(col("order_status") === 3), // 只统计已完成订单
        Seq("order_id", "dt"))
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select(
        "product_id", "product_name", "category_id", "category_name",
        "min_stock", "max_stock", "reorder_point", "safety_stock"
      )
    
    // 计算日均销量
    val dailySalesDf = orderDetailsDf
      .groupBy("product_id", "dt")
      .agg(
        sum("quantity").as("daily_sales")
      )
      .groupBy("product_id")
      .agg(
        avg("daily_sales").as("avg_daily_sales")
      )
    
    // 计算当前库存
    val currentStockDf = inventoryChangesDf
      .groupBy("product_id")
      .agg(
        sum("change_quantity").as("current_stock")
      )
    
    // 计算补货次数
    val reorderTimesDf = inventoryChangesDf
      .where(col("change_type") === 1) // 假设1表示入库
      .groupBy("product_id", "dt")
      .agg(
        count("*").as("daily_reorder_times")
      )
      .groupBy("product_id")
      .agg(
        sum("daily_reorder_times").as("reorder_times")
      )
    
    // 计算库存周转指标
    val turnoverDf = currentStockDf
      .join(dailySalesDf, Seq("product_id"))
      .withColumn("turnover_days",
        when(col("avg_daily_sales") > 0,
          col("current_stock") / col("avg_daily_sales"))
        .otherwise(999999)) // 如果没有销量，设置一个极大值
      .withColumn("turnover_rate",
        when(col("current_stock") > 0,
          col("avg_daily_sales") / col("current_stock"))
        .otherwise(0.0))
    
    // 计算库存状态
    val stockStatusDf = turnoverDf
      .join(productDf, Seq("product_id"))
      .withColumn("stock_status",
        when(col("current_stock") <= col("safety_stock"), "库存不足")
        .when(col("current_stock") <= col("reorder_point"), "需要补货")
        .when(col("current_stock") > col("max_stock"), "库存积压")
        .otherwise("库存正常"))
      .withColumn("stock_health_score",
        when(col("current_stock") === 0, 0.0)
        .when(col("current_stock") <= col("safety_stock"), 40.0)
        .when(col("current_stock") <= col("reorder_point"), 60.0)
        .when(col("current_stock") <= col("max_stock"), 100.0)
        .when(col("current_stock") > col("max_stock") * 1.5, 40.0)
        .when(col("current_stock") > col("max_stock"), 70.0)
        .otherwise(90.0))
    
    // 计算库存效率指标
    val inventoryEfficiencyDf = stockStatusDf
      .join(reorderTimesDf, Seq("product_id"))
      .withColumn("inventory_efficiency_score",
        (when(col("turnover_days") <= 30, 100.0)
         .when(col("turnover_days") <= 60, 80.0)
         .when(col("turnover_days") <= 90, 60.0)
         .otherwise(40.0) * 0.4 +  // 周转天数权重40%
         col("stock_health_score") * 0.4 +  // 库存健康度权重40%
         when(col("reorder_times") <= 5, 100.0)
         .when(col("reorder_times") <= 10, 80.0)
         .when(col("reorder_times") <= 15, 60.0)
         .otherwise(40.0) * 0.2))  // 补货次数权重20%
    
    // 计算类目维度的库存周转情况
    val categoryTurnoverDf = inventoryEfficiencyDf
      .groupBy("category_id", "category_name", "dt")
      .agg(
        avg("turnover_days").as("avg_turnover_days"),
        avg("turnover_rate").as("avg_turnover_rate"),
        avg("inventory_efficiency_score").as("avg_efficiency_score"),
        count(when(col("stock_status") === "库存不足", 1)).as("stockout_count"),
        count(when(col("stock_status") === "库存积压", 1)).as("overstock_count")
      )
    
    // 计算同环比
    val inventoryMetricsWithGrowthDf = calculateGrowthRate(
      inventoryEfficiencyDf,
      Seq("product_id"),
      "turnover_rate"
    )
    
    // 添加ETL时间
    addETLColumns(inventoryMetricsWithGrowthDf)
  }

  /**
   * 处理库存周转汇总数据
   */
  override def process(): Unit = {
    try {
      // 处理库存周转数据
      val inventoryTurnoverDf = processInventoryTurnover()
      
      // 保存到Hive表
      saveToHive(inventoryTurnoverDf, "dws.dws_inventory_turnover")
      println("库存周转汇总表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"库存周转汇总表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 