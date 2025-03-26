package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 库存周转分析处理类
 */
class InventoryTurnoverProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_inventory_turnover_stats_m"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的库存和商品数据（按月汇总）
    val inventoryData = spark.sql(
      s"""
         |SELECT 
         |  i.product_id,
         |  p.category_name,
         |  i.current_stock,
         |  i.safety_stock,
         |  i.max_stock,
         |  s.avg_daily_sales,
         |  substr(i.dt, 1, 7) as dt
         |FROM dws.dws_inventory i
         |JOIN dws.dws_product p ON i.product_id = p.product_id AND i.dt = p.dt
         |LEFT JOIN (
         |  SELECT 
         |    product_id,
         |    avg(daily_sales) as avg_daily_sales,
         |    dt
         |  FROM dws.dws_product_sales
         |  WHERE substr(dt, 1, 7) = substr('$dt', 1, 7)
         |  GROUP BY product_id, dt
         |) s ON i.product_id = s.product_id AND i.dt = s.dt
         |WHERE substr(i.dt, 1, 7) = substr('$dt', 1, 7)
         |""".stripMargin)
    
    Map("inventory" -> inventoryData)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val inventory = data("inventory")
    
    // 1. 计算每个商品的周转指标
    val withTurnover = inventory
      // 计算周转天数
      .withColumn("turnover_days",
        when(col("avg_daily_sales") > 0,
          col("current_stock") / col("avg_daily_sales")
        ).otherwise(999999.0)
      )
      // 计算周转率（月周转次数）
      .withColumn("turnover_rate",
        when(col("turnover_days") > 0,
          30 / col("turnover_days")
        ).otherwise(0.0)
      )
      // 计算效率得分
      .withColumn("efficiency_score",
        when(col("turnover_days") <= 7, 100.0)
          .when(col("turnover_days") <= 30, 80.0)
          .when(col("turnover_days") <= 60, 60.0)
          .when(col("turnover_days") <= 90, 40.0)
          .otherwise(20.0)
      )
      // 判断库存状态
      .withColumn("is_stockout",
        when(col("current_stock") <= col("safety_stock"), 1).otherwise(0)
      )
      .withColumn("is_overstock",
        when(col("current_stock") >= col("max_stock"), 1).otherwise(0)
      )
    
    // 2. 按类目分组统计
    val categoryStats = withTurnover
      .groupBy("dt", "category_name")
      .agg(
        avg("turnover_days").as("avg_turnover_days"),
        avg("turnover_rate").as("avg_turnover_rate"),
        avg("efficiency_score").as("avg_efficiency_score"),
        sum("is_stockout").as("stockout_count"),
        sum("is_overstock").as("overstock_count")
      )
    
    // 3. 计算环比增长率
    val withMoM = calculateMoM(categoryStats, "avg_turnover_rate")
    
    // 4. 四舍五入数值字段
    withMoM.select(
      col("dt"),
      col("category_name"),
      round(col("avg_turnover_days"), 2).as("avg_turnover_days"),
      round(col("avg_turnover_rate"), 4).as("avg_turnover_rate"),
      round(col("avg_efficiency_score"), 2).as("avg_efficiency_score"),
      col("stockout_count"),
      col("overstock_count"),
      round(col("mom_rate"), 4).as("mom_rate")
    )
  }
}