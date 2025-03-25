package com.sales.etl

import com.sales.etl.dimension.{UserDimensionProcessor, ProductDimensionProcessor}
import com.sales.etl.fact.{SalesFactProcessor, InventoryFactProcessor}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.time.LocalDate

object SalesDataETL {
  private val logger = LoggerFactory.getLogger(getClass)
  
  def main(args: Array[String]): Unit = {
    // 解析参数
    val date = if (args.length > 0) args(0) else LocalDate.now().minusDays(1).toString
    
    // 初始化Spark
    val spark = SparkSession.builder()
      .appName("Sales Data ETL")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // 处理维度表
      logger.info("Processing dimension tables...")
      processDimensions(spark, date)
      
      // 处理事实表
      logger.info("Processing fact tables...")
      processFacts(spark, date)
      
      // 计算指标
      logger.info("Calculating metrics...")
      calculateMetrics(spark, date)
      
    } catch {
      case e: Exception =>
        logger.error("ETL process failed", e)
        throw e
    } finally {
      spark.stop()
    }
  }
  
  private def processDimensions(spark: SparkSession, date: String): Unit = {
    // 处理用户维度
    val userProcessor = new UserDimensionProcessor(spark)
    userProcessor.process(date)
    
    // 处理产品维度
    val productProcessor = new ProductDimensionProcessor(spark)
    productProcessor.process(date)
  }
  
  private def processFacts(spark: SparkSession, date: String): Unit = {
    // 处理销售事实
    val salesProcessor = new SalesFactProcessor(spark)
    salesProcessor.process(date)
    
    // 处理库存事实
    val inventoryProcessor = new InventoryFactProcessor(spark)
    inventoryProcessor.process(date)
  }
  
  private def calculateMetrics(spark: SparkSession, date: String): Unit = {
    val inventoryProcessor = new InventoryFactProcessor(spark)
    
    // 计算库存周转率
    logger.info("Calculating inventory turnover...")
    val startDate = LocalDate.parse(date).minusDays(30).toString
    val turnoverDF = inventoryProcessor.calculateInventoryTurnover(startDate, date)
    
    // 保存库存周转率
    turnoverDF.write
      .mode("overwrite")
      .partitionBy("dt")
      .saveAsTable("dws_inventory_turnover")
    
    // 计算安全库存水平
    logger.info("Calculating safety stock levels...")
    val safetyStockDF = inventoryProcessor.calculateSafetyStock(90) // 使用90天的数据计算
    
    // 保存安全库存水平
    safetyStockDF.write
      .mode("overwrite")
      .partitionBy("dt")
      .saveAsTable("dws_safety_stock")
    
    // 计算销售汇总指标
    logger.info("Calculating sales summary metrics...")
    spark.sql(s"""
      INSERT OVERWRITE TABLE dws_sales_summary
      PARTITION (dt = '$date')
      SELECT 
        date_key,
        product_key,
        count(distinct user_key) as distinct_customers,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_quantity,
        sum(total_amount) as total_sales_amount,
        sum(discount_amount) as total_discount_amount,
        sum(final_amount) as total_final_amount,
        sum(final_amount) / count(distinct user_key) as avg_customer_spend
      FROM dwd_fact_sales
      WHERE dt = '$date'
      GROUP BY date_key, product_key
    """)
  }
} 