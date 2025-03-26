package com.bigdata.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 库存变动事实表处理类
 */
class InventoryFactProcessor(spark: SparkSession) extends FactProcessor(spark) {
  import spark.implicits._

  /**
   * 处理库存变动数据
   */
  private def processInventoryChanges(): DataFrame = {
    // 读取库存变动日志表
    val inventoryLogDf = readFromODS("ods_inventory_log")
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select("product_id", "product_name", "category_id", "supplier_id")
    
    // 关联商品维度信息
    val inventoryWithProductDf = inventoryLogDf
      .join(productDf, Seq("product_id"), "left")
    
    // 计算变动前后的库存量
    val inventoryChangesDf = inventoryWithProductDf
      .withColumn("change_quantity", 
        when(col("change_type") === 1, col("quantity"))  // 入库
        .when(col("change_type") === 2, -col("quantity")) // 出库
        .otherwise(col("quantity")))
      .withColumn("before_quantity", 
        coalesce(lag("after_quantity", 1).over(
          Window.partitionBy("product_id").orderBy("created_at")), lit(0)))
      .withColumn("after_quantity", 
        col("before_quantity") + col("change_quantity"))
    
    // 添加ETL时间和分区字段
    addETLColumns(inventoryChangesDf)
  }

  /**
   * 处理库存相关的所有事实表数据
   */
  override def process(): Unit = {
    try {
      // 处理库存变动表
      val inventoryChangesDf = processInventoryChanges()
      saveToHive(inventoryChangesDf, "dwd.dwd_fact_inventory_change")
      println("库存变动事实表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"库存变动事实表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 