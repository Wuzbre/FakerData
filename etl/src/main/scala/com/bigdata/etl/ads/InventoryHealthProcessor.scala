package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 库存健康状况处理类
 */
class InventoryHealthProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_inventory_health_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的库存和销售数据
    val inventoryData = spark.sql(
      s"""
         |SELECT 
         |  i.product_id,
         |  p.product_name,
         |  i.current_stock,
         |  i.safety_stock,
         |  i.max_stock,
         |  s.avg_daily_sales,
         |  i.dt
         |FROM dws.dws_inventory i
         |JOIN dws.dws_product p ON i.product_id = p.product_id AND i.dt = p.dt
         |LEFT JOIN (
         |  SELECT 
         |    product_id,
         |    avg(daily_sales) as avg_daily_sales,
         |    dt
         |  FROM dws.dws_product_sales
         |  WHERE dt >= date_sub('$dt', 30)
         |  GROUP BY product_id, dt
         |) s ON i.product_id = s.product_id AND i.dt = s.dt
         |WHERE i.dt = '$dt'
         |""".stripMargin)
    
    Map("inventory" -> inventoryData)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val inventory = data("inventory")
    
    // 1. 计算周转天数
    val withTurnover = inventory
      .withColumn("turnover_days",
        when(col("avg_daily_sales") > 0,
          col("current_stock") / col("avg_daily_sales")
        ).otherwise(999999.0) // 如果没有销量，设置一个很大的数表示积压
      )
    
    // 2. 判断库存状态
    val withStatus = withTurnover
      .withColumn("stock_status",
        when(col("current_stock") <= col("safety_stock"), "缺货风险")
          .when(col("current_stock") >= col("max_stock"), "库存积压")
          .when(col("turnover_days") > 90, "周转不畅") // 超过90天未售出
          .otherwise("正常")
      )
    
    // 3. 计算库存健康得分
    // 得分 = 标准化库存水平 * 0.4 + 标准化周转效率 * 0.4 + 安全库存达标率 * 0.2
    val withScore = withStatus
      // 库存水平得分：过高或过低都扣分
      .withColumn("stock_level_score",
        when(col("current_stock") <= col("safety_stock"), 
          col("current_stock") / col("safety_stock") * 100
        ).when(col("current_stock") >= col("max_stock"),
          (lit(2) * col("max_stock") - col("current_stock")) / col("max_stock") * 100
        ).otherwise(100.0)
      )
      // 周转效率得分：天数越少分数越高
      .withColumn("turnover_score",
        when(col("turnover_days") <= 7, 100.0) // 周转天数<=7天，满分
          .when(col("turnover_days") <= 30, 80.0) // <=30天，80分
          .when(col("turnover_days") <= 60, 60.0) // <=60天，60分
          .when(col("turnover_days") <= 90, 40.0) // <=90天，40分
          .otherwise(20.0) // >90天，20分
      )
      // 安全库存达标得分
      .withColumn("safety_score",
        when(col("current_stock") >= col("safety_stock"), 100.0)
          .otherwise(col("current_stock") / col("safety_stock") * 100)
      )
      // 计算最终得分
      .withColumn("stock_health_score",
        (col("stock_level_score") * 0.4 +
         col("turnover_score") * 0.4 +
         col("safety_score") * 0.2)
      )
    
    // 4. 四舍五入数值字段
    withScore.select(
      col("dt"),
      col("product_id"),
      col("product_name"),
      col("current_stock"),
      round(col("avg_daily_sales"), 2).as("avg_daily_sales"),
      round(col("turnover_days"), 2).as("turnover_days"),
      col("stock_status"),
      round(col("stock_health_score"), 2).as("stock_health_score")
    )
  }
} 