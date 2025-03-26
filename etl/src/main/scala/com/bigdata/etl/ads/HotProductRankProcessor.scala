package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 热销商品排行处理类
 */
class HotProductRankProcessor(spark: SparkSession) extends AdsProcessor(spark) {
  
  override protected def getTargetTable: String = "ads.ads_hot_product_rank_d"
  
  override protected def prepareData(): Map[String, DataFrame] = {
    // 1. 读取DWS层的商品销售汇总数据
    val productSales = spark.sql(
      s"""
         |SELECT 
         |  p.product_id,
         |  p.product_name,
         |  c.category_name,
         |  p.total_quantity,
         |  p.total_amount,
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
    
    // 1. 计算商品热度得分
    // 热度得分 = 标准化销量 * 0.4 + 标准化销售额 * 0.3 + 标准化评分 * 0.3
    val withNormalized = productSales
      .withColumn("norm_quantity", 
        (col("total_quantity") - min("total_quantity").over()) / 
        (max("total_quantity").over() - min("total_quantity").over()))
      .withColumn("norm_amount",
        (col("total_amount") - min("total_amount").over()) / 
        (max("total_amount").over() - min("total_amount").over()))
      .withColumn("norm_rating",
        (col("avg_rating") - min("avg_rating").over()) / 
        (max("avg_rating").over() - min("avg_rating").over()))
      .withColumn("popularity_score",
        col("norm_quantity") * 0.4 + 
        col("norm_amount") * 0.3 + 
        col("norm_rating") * 0.3)
    
    // 2. 计算排名
    val withRank = calculateRank(withNormalized, "popularity_score")
    
    // 3. 四舍五入数值字段
    withRank.select(
      col("dt"),
      col("product_id"),
      col("product_name"),
      col("category_name"),
      col("total_quantity"),
      round(col("total_amount"), 2).as("total_amount"),
      round(col("avg_rating"), 2).as("avg_rating"),
      round(col("popularity_score"), 2).as("popularity_score"),
      col("rank_num")
    )
  }
} 