package com.bigdata.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 价格变动事实表处理类
 */
class PriceFactProcessor(spark: SparkSession) extends FactProcessor(spark) {
  import spark.implicits._

  /**
   * 处理价格变动数据
   */
  private def processPriceChanges(): DataFrame = {
    // 读取价格变动历史表
    val priceHistoryDf = readFromODS("ods_price_history")
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select("product_id", "product_name", "category_id", "supplier_id")
    
    // 关联商品维度信息
    val priceWithProductDf = priceHistoryDf
      .join(productDf, Seq("product_id"), "left")
    
    // 计算价格变动信息
    val priceChangesDf = priceWithProductDf
      .withColumn("old_price", 
        lag("new_price", 1).over(
          Window.partitionBy("product_id").orderBy("created_at")))
      .withColumn("price_change_rate", 
        when(col("old_price").isNotNull, 
          (col("new_price") - col("old_price")) / col("old_price"))
        .otherwise(lit(0)))
      .withColumn("change_reason",
        when(col("price_change_rate") > 0, "涨价")
        .when(col("price_change_rate") < 0, "降价")
        .otherwise("价格不变"))
    
    // 添加ETL时间和分区字段
    addETLColumns(priceChangesDf)
  }

  /**
   * 处理价格相关的所有事实表数据
   */
  override def process(): Unit = {
    try {
      // 处理价格变动表
      val priceChangesDf = processPriceChanges()
      saveToHive(priceChangesDf, "dwd.dwd_fact_price_change")
      println("价格变动事实表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"价格变动事实表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 