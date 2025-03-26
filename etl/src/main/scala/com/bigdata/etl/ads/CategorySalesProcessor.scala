package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 商品类目销售分析处理类
 */
class CategorySalesProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_category_sales_stats_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的商品销售数据和类目数据
    val productSales = spark.sql(
      s"""
         |SELECT 
         |  p.category_id,
         |  c.category_name,
         |  p.total_amount,
         |  p.total_quantity,
         |  p.avg_rating,
         |  p.dt
         |FROM dws.dws_product_sales p
         |JOIN dws.dws_category c ON p.category_id = c.category_id AND p.dt = c.dt
         |WHERE p.dt = '$dt'
         |""".stripMargin)
    
    Map("product_sales" -> productSales)
  }
  
  override protected def transform(data: Map[String, DataFrame]): DataFrame = {
    val productSales = data("product_sales")
    
    // 1. 计算每个类目的销售总额
    val categoryTotalAmount = productSales
      .groupBy("dt")
      .agg(sum("total_amount").as("all_category_amount"))
    
    // 2. 按类目分组统计
    val categorySales = productSales
      .groupBy("dt", "category_name")
      .agg(
        sum("total_amount").as("total_amount"),
        sum("total_quantity").as("total_quantity"),
        avg("avg_rating").as("avg_rating"),
        count("*").as("product_count")
      )
    
    // 3. 计算销售占比
    val withRatio = categorySales
      .join(categoryTotalAmount, "dt")
      .withColumn("sales_ratio", 
        when(col("all_category_amount") > 0,
          col("total_amount") / col("all_category_amount")
        ).otherwise(0.0)
      )
    
    // 4. 四舍五入数值字段
    withRatio.select(
      col("dt"),
      col("category_name"),
      round(col("total_amount"), 2).as("total_amount"),
      col("total_quantity"),
      round(col("avg_rating"), 2).as("avg_rating"),
      col("product_count"),
      round(col("sales_ratio"), 4).as("sales_ratio")
    )
  }
} 