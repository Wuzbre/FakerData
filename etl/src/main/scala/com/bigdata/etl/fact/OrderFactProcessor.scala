package com.bigdata.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 订单事实表处理类
 */
class OrderFactProcessor(spark: SparkSession) extends FactProcessor(spark) {
  import spark.implicits._

  /**
   * 处理订单主表数据
   */
  private def processOrders(): DataFrame = {
    // 读取订单表数据
    val ordersDf = readFromODS("ods_orders")
      
    // 读取用户维度表
    val userDf = readFromDIM("dim_user")
      .select("user_id", "region", "city")
    
    // 关联用户维度信息
    val ordersWithUserDf = ordersDf
      .join(userDf, Seq("user_id"), "left")
    
    // 判断是否首单
    val windowSpec = Window.partitionBy("user_id").orderBy("created_at")
    val ordersWithFirstFlagDf = ordersWithUserDf
      .withColumn("order_rank", row_number().over(windowSpec))
      .withColumn("is_first_order", col("order_rank") === 1)
      .drop("order_rank")
    
    // 添加ETL时间和分区字段
    addETLColumns(ordersWithFirstFlagDf)
  }

  /**
   * 处理订单明细表数据
   */
  private def processOrderDetails(): DataFrame = {
    // 读取订单明细表数据
    val orderDetailsDf = readFromODS("ods_order_details")
    
    // 读取商品维度表
    val productDf = readFromDIM("dim_product")
      .select("product_id", "product_name", "category_id", "supplier_id")
    
    // 关联商品维度信息
    val detailsWithProductDf = orderDetailsDf
      .join(productDf, Seq("product_id"), "left")
    
    // 计算折扣金额
    val detailsWithDiscountDf = detailsWithProductDf
      .withColumn("discount_amount", 
        when(col("is_promotion_price") === true, 
          col("original_price") * col("quantity") - col("total_price"))
        .otherwise(lit(0)))
    
    // 添加ETL时间和分区字段
    addETLColumns(detailsWithDiscountDf)
  }

  /**
   * 处理订单相关的所有事实表数据
   */
  override def process(): Unit = {
    try {
      // 处理订单主表
      val ordersDf = processOrders()
      saveToHive(ordersDf, "dwd.dwd_fact_order")
      println("订单事实表处理完成")

      // 处理订单明细表
      val orderDetailsDf = processOrderDetails()
      saveToHive(orderDetailsDf, "dwd.dwd_fact_order_detail")
      println("订单明细事实表处理完成")
      
    } catch {
      case e: Exception =>
        println(s"订单事实表处理失败: ${e.getMessage}")
        throw e
    }
  }
} 