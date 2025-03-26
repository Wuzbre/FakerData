package com.bigdata.etl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.typesafe.config.ConfigFactory

object DataExtractor {
  // 加载配置文件
  private val config = ConfigFactory.load()
  private val mysqlUrl = config.getString("mysql.url")
  private val mysqlDriver = config.getString("mysql.driver")
  private val mysqlUser = config.getString("mysql.user")
  private val mysqlPassword = config.getString("mysql.password")
  private val hiveWarehouse = config.getString("hive.warehouse")
  private val batchSize = config.getInt("etl.batch_size")

  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Data Extraction")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", hiveWarehouse)
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .getOrCreate()

    // 设置日志级别
    spark.sparkContext.setLogLevel("WARN")

    // 获取当前时间作为ETL时间
    val etlTime = LocalDateTime.now()
    val etlTimeStr = etlTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

    try {
      // 抽取订单数据
      extractOrders(spark, etlTimeStr)
      
      // 抽取订单明细数据
      extractOrderDetails(spark, etlTimeStr)
      
      // 抽取商品数据
      extractProducts(spark, etlTimeStr)
      
      // 抽取用户数据
      extractUsers(spark, etlTimeStr)
      
      // 抽取供应商数据
      extractSuppliers(spark, etlTimeStr)
      
      // 抽取促销数据
      extractPromotions(spark, etlTimeStr)
      
      // 抽取库存日志数据
      extractInventoryLogs(spark, etlTimeStr)
      
      // 抽取价格历史数据
      extractPriceHistory(spark, etlTimeStr)
      
    } catch {
      case e: Exception =>
        println(s"数据抽取过程中发生错误: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * 从MySQL读取数据的通用方法
   */
  private def readFromMysql(spark: SparkSession, tableName: String): DataFrame = {
    spark.read
      .format("jdbc")
      .option("url", mysqlUrl)
      .option("driver", mysqlDriver)
      .option("dbtable", tableName)
      .option("user", mysqlUser)
      .option("password", mysqlPassword)
      .option("fetchsize", batchSize)
      .load()
  }

  /**
   * 写入Hive表的通用方法
   */
  private def writeToHive(df: DataFrame, tableName: String): Unit = {
    df.write
      .format("hive")
      .partitionBy("dt")
      .mode("overwrite")
      .option("path", s"${hiveWarehouse}/${tableName}")
      .saveAsTable(s"${hiveWarehouse}.${tableName}")
  }

  /**
   * 抽取订单数据
   */
  def extractOrders(spark: SparkSession, etlTime: String): Unit = {
    val orderDF = readFromMysql(spark, "`order`")
    
    val ordersWithETL = orderDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("created_at"), "yyyy-MM-dd"))

    writeToHive(ordersWithETL, "ods_order")
  }

  /**
   * 抽取订单明细数据
   */
  def extractOrderDetails(spark: SparkSession, etlTime: String): Unit = {
    val orderDetailDF = readFromMysql(spark, "order_detail")
    
    val orderDetailsWithETL = orderDetailDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("created_at"), "yyyy-MM-dd"))

    writeToHive(orderDetailsWithETL, "ods_order_detail")
  }

  /**
   * 抽取商品数据
   */
  def extractProducts(spark: SparkSession, etlTime: String): Unit = {
    val productDF = readFromMysql(spark, "product")
    
    val productsWithETL = productDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("created_at"), "yyyy-MM-dd"))

    writeToHive(productsWithETL, "ods_product")
  }

  /**
   * 抽取用户数据
   */
  def extractUsers(spark: SparkSession, etlTime: String): Unit = {
    val userDF = readFromMysql(spark, "user")
    
    val usersWithETL = userDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("register_time"), "yyyy-MM-dd"))

    writeToHive(usersWithETL, "ods_user")
  }

  /**
   * 抽取供应商数据
   */
  def extractSuppliers(spark: SparkSession, etlTime: String): Unit = {
    val supplierDF = readFromMysql(spark, "supplier")
    
    val suppliersWithETL = supplierDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("cooperation_start_date"), "yyyy-MM-dd"))

    writeToHive(suppliersWithETL, "ods_supplier")
  }

  /**
   * 抽取促销数据
   */
  def extractPromotions(spark: SparkSession, etlTime: String): Unit = {
    val promotionDF = readFromMysql(spark, "promotion")
    
    val promotionsWithETL = promotionDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("start_time"), "yyyy-MM-dd"))

    writeToHive(promotionsWithETL, "ods_promotion")
  }

  /**
   * 抽取库存日志数据
   */
  def extractInventoryLogs(spark: SparkSession, etlTime: String): Unit = {
    val inventoryLogDF = readFromMysql(spark, "inventory_log")
    
    val inventoryLogsWithETL = inventoryLogDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("created_at"), "yyyy-MM-dd"))

    writeToHive(inventoryLogsWithETL, "ods_inventory_log")
  }

  /**
   * 抽取价格历史数据
   */
  def extractPriceHistory(spark: SparkSession, etlTime: String): Unit = {
    val priceHistoryDF = readFromMysql(spark, "price_history")
    
    val priceHistoryWithETL = priceHistoryDF
      .withColumn("etl_time", lit(etlTime))
      .withColumn("dt", date_format(col("created_at"), "yyyy-MM-dd"))

    writeToHive(priceHistoryWithETL, "ods_price_history")
  }
} 