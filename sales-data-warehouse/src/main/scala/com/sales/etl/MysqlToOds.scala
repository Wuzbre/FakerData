package com.sales.etl

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.sales.utils.{ConfigUtils, DateUtils, SparkSessionUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util.Properties
import org.slf4j.LoggerFactory
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.log4j.Logger
import scala.util.{Try, Success, Failure}

/**
 * MySQL数据抽取到ODS层的组件
 */
object MysqlToOds {
  
  private val logger = Logger.getLogger(this.getClass)
  
  /**
   * 执行MySQL到ODS的ETL过程
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  def process(spark: SparkSession, dt: String): Boolean = {
    try {
      logger.info(s"开始MySQL到ODS层数据抽取，处理日期：$dt")
      
      // 确保ODS数据库存在
      spark.sql(s"CREATE DATABASE IF NOT EXISTS ${ConfigUtils.ODS_DATABASE}")
      spark.sql(s"USE ${ConfigUtils.ODS_DATABASE}")
      
      // 抽取各表数据
      val status = extractUsers(spark, dt) &&
              extractEmployees(spark, dt) &&
              extractProducts(spark, dt) &&
              extractSalesOrders(spark, dt) &&
              extractSalesOrderItems(spark, dt) &&
              extractPurchaseOrders(spark, dt) &&
              extractPurchaseOrderItems(spark, dt) &&
              extractSuppliers(spark, dt)
      
      logger.info(s"MySQL到ODS层数据抽取${ if (status) "成功" else "失败" }，处理日期：$dt")
      status
    } catch {
      case e: Exception =>
        logger.error(s"MySQL到ODS层数据抽取异常：${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 通用的MySQL表数据抽取方法，优化了增量抽取策略
   *
   * @param spark SparkSession
   * @param mysqlTable MySQL表名
   * @param odsTable ODS表名
   * @param dt 处理日期
   * @param whereClause 增量条件
   * @return 抽取是否成功
   */
  private def extractTable(
    spark: SparkSession, 
    mysqlTable: String, 
    odsTable: String, 
    dt: String,
    whereClause: String = ""
  ): Boolean = {
    try {
      // 获取处理日期对应的日期范围
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val processingDate = dateFormat.parse(dt)
      
      // 创建日历对象并设置为处理日期
      val calendar = Calendar.getInstance()
      calendar.setTime(processingDate)
      
      // 设置为当天起始时间（00:00:00）
      calendar.set(Calendar.HOUR_OF_DAY, 0)
      calendar.set(Calendar.MINUTE, 0)
      calendar.set(Calendar.SECOND, 0)
      val startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime)
      
      // 设置为当天结束时间（23:59:59）
      calendar.set(Calendar.HOUR_OF_DAY, 23)
      calendar.set(Calendar.MINUTE, 59)
      calendar.set(Calendar.SECOND, 59)
      val endTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(calendar.getTime)
      
      // 构建准确的日期范围条件，确保数据不会漏抽或重抽
      val dateCondition = s"(updated_at BETWEEN '$startTime' AND '$endTime' OR created_at BETWEEN '$startTime' AND '$endTime')"
      val finalWhereClause = if (whereClause.nonEmpty) s"$whereClause AND $dateCondition" else dateCondition
      
      // 构建查询语句
      val query = s"(SELECT *, NOW() as etl_time FROM $mysqlTable WHERE $finalWhereClause) as tmp"
      
      // 从MySQL读取数据
      val df = spark.read
        .format("jdbc")
        .option("url", ConfigUtils.MYSQL_JDBC_URL)
        .option("dbtable", query)
        .option("user", ConfigUtils.MYSQL_USER)
        .option("password", ConfigUtils.MYSQL_PASSWORD)
        .option("driver", ConfigUtils.MYSQL_DRIVER)
        .load()
        
      // 记录抽取的数据量
      val extractCount = df.count()
      logger.info(s"从MySQL表 $mysqlTable 抽取了 $extractCount 条记录，日期范围: $startTime 至 $endTime")
      
      // 创建表（如果不存在）并写入数据
      if (extractCount > 0) {
        createOdsTable(spark, df, odsTable)
        df.withColumn("dt", lit(dt))
          .write
          .format("hive")
          .mode(SaveMode.Append)
          .partitionBy("dt")
          .saveAsTable(s"${ConfigUtils.ODS_DATABASE}.$odsTable")
      } else {
        logger.warn(s"MySQL表 $mysqlTable 在日期范围内没有数据，跳过写入")
      }
      
      true
    } catch {
      case e: Exception =>
        logger.error(s"抽取MySQL表 $mysqlTable 到ODS表 $odsTable 失败：${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 创建ODS表（如果不存在）
   *
   * @param spark SparkSession
   * @param df 数据框
   * @param tableName 表名
   */
  private def createOdsTable(spark: SparkSession, df: DataFrame, tableName: String): Unit = {
    // 为DataFrame添加分区字段
    val partitionDt = DateUtils.getPartitionDt(DateUtils.getTodayStr())
    val dfWithPartition = df.withColumn("dt", lit(partitionDt))
    
    // 创建表
    dfWithPartition.write
      .format("hive")
      .mode(SaveMode.Append)
      .option("path", s"${ConfigUtils.HIVE_WAREHOUSE_DIR}/${ConfigUtils.ODS_DATABASE}.db/$tableName")
      .partitionBy("dt")
      .saveAsTable(s"${ConfigUtils.ODS_DATABASE}.$tableName")
  }
  
  /**
   * 抽取users表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractUsers(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_USERS, 
      "ods_users", 
      dt
    )
  }
  
  /**
   * 抽取employees表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractEmployees(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_EMPLOYEES, 
      "ods_employees", 
      dt
    )
  }
  
  /**
   * 抽取products表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractProducts(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_PRODUCTS, 
      "ods_products", 
      dt
    )
  }
  
  /**
   * 抽取sales_orders表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractSalesOrders(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_SALES_ORDERS, 
      "ods_sales_orders", 
      dt
    )
  }
  
  /**
   * 抽取sales_order_items表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractSalesOrderItems(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_SALES_ORDER_ITEMS, 
      "ods_sales_order_items", 
      dt
    )
  }
  
  /**
   * 抽取purchase_orders表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractPurchaseOrders(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_PURCHASE_ORDERS, 
      "ods_purchase_orders", 
      dt
    )
  }
  
  /**
   * 抽取purchase_order_items表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractPurchaseOrderItems(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_PURCHASE_ORDER_ITEMS, 
      "ods_purchase_order_items", 
      dt
    )
  }
  
  /**
   * 抽取suppliers表数据
   *
   * @param spark SparkSession
   * @param dt 处理日期
   * @return 是否成功
   */
  private def extractSuppliers(spark: SparkSession, dt: String): Boolean = {
    extractTable(
      spark, 
      ConfigUtils.TABLE_SUPPLIERS, 
      "ods_suppliers", 
      dt
    )
  }
  
  /**
   * 清理ODS层过期分区
   * @param spark SparkSession
   * @param retentionDays 保留天数
   * @return 是否成功
   */
  def cleanExpiredPartitions(spark: SparkSession, retentionDays: Int = 90): Boolean = {
    try {
      logger.info(s"开始清理ODS层过期分区，保留 $retentionDays 天数据")
      
      // 计算过期日期
      val cal = Calendar.getInstance()
      cal.add(Calendar.DAY_OF_MONTH, -retentionDays)
      val expireDate = new SimpleDateFormat("yyyyMMdd").format(cal.getTime())
      
      // 获取所有ODS表
      val tables = spark.sql(s"SHOW TABLES IN ${ConfigUtils.ODS_DATABASE}").collect()
        .map(row => row.getString(1))
        .filter(_.startsWith("ods_"))
      
      // 对每个表清理过期分区
      tables.foreach { tableName =>
        try {
          // 获取表的分区
          val partitions = spark.sql(s"SHOW PARTITIONS ${ConfigUtils.ODS_DATABASE}.$tableName").collect()
            .map(row => row.getString(0).split("=")(1))
            .filter(_ <= expireDate)
          
          // 删除过期分区
          partitions.foreach { partition =>
            spark.sql(s"ALTER TABLE ${ConfigUtils.ODS_DATABASE}.$tableName DROP PARTITION (dt='$partition')")
            logger.info(s"已删除表 $tableName 的过期分区 dt=$partition")
          }
        } catch {
          case e: Exception =>
            logger.warn(s"清理表 $tableName 的过期分区时出错: ${e.getMessage}")
        }
      }
      
      logger.info("ODS层过期分区清理完成")
      true
    } catch {
      case e: Exception =>
        logger.error(s"清理ODS层过期分区时发生异常: ${e.getMessage}", e)
        false
    }
  }
} 