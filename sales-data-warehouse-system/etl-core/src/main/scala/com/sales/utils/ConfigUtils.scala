package com.sales.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scala.util.{Try, Success, Failure}
import java.util.Properties
import java.io.FileInputStream
import scala.collection.JavaConverters._

/**
 * 配置工具类
 * 用于管理整个数据仓库项目的配置参数，包括数据库连接、表名、路径等
 */
object ConfigUtils {
  private val logger = Logger.getLogger(this.getClass)
  
  private val config: Config = ConfigFactory.load()
  
  // MySQL配置
  val MYSQL_JDBC_URL: String = config.getString("sales.mysql.jdbc.url")
  val MYSQL_USER: String = config.getString("sales.mysql.user")
  val MYSQL_PASSWORD: String = config.getString("sales.mysql.password")
  val MYSQL_DRIVER: String = config.getString("sales.mysql.driver")
  
  // Hive配置
  val HIVE_WAREHOUSE_DIR: String = config.getString("sales.hive.warehouse.dir")
  val HIVE_METASTORE_URIS: String = config.getString("sales.hive.metastore.uris")
  val ODS_DATABASE: String = config.getString("sales.hive.ods.database")
  val DWD_DATABASE: String = config.getString("sales.hive.dwd.database")
  val DWS_DATABASE: String = config.getString("sales.hive.dws.database")
  val DQ_DATABASE: String = config.getString("sales.hive.dq.database")
  
  // Doris配置
  val DORIS_JDBC_URL: String = config.getString("sales.doris.jdbc.url")
  val DORIS_USER: String = config.getString("sales.doris.user")
  val DORIS_PASSWORD: String = config.getString("sales.doris.password")
  val DORIS_DATABASE: String = config.getString("sales.doris.database")
  
  // 表配置
  val TABLE_USERS: String = config.getString("sales.tables.users")
  val TABLE_EMPLOYEES: String = config.getString("sales.tables.employees")
  val TABLE_PRODUCTS: String = config.getString("sales.tables.products")
  val TABLE_SALES_ORDERS: String = config.getString("sales.tables.sales_orders")
  val TABLE_SALES_ORDER_ITEMS: String = config.getString("sales.tables.sales_order_items")
  val TABLE_PURCHASE_ORDERS: String = config.getString("sales.tables.purchase_orders")
  val TABLE_PURCHASE_ORDER_ITEMS: String = config.getString("sales.tables.purchase_order_items")
  val TABLE_SUPPLIERS: String = config.getString("sales.tables.suppliers")
  
  // 数据质量检查阈值
  val NULL_RATIO_THRESHOLD: Double = config.getDouble("sales.etl.quality.null.ratio.threshold")
  val COMPLETENESS_THRESHOLD: Double = config.getDouble("sales.etl.quality.completeness.threshold")
  val CONSISTENCY_THRESHOLD: Double = config.getDouble("sales.etl.quality.consistency.threshold")
  val ACCURACY_THRESHOLD: Double = config.getDouble("sales.etl.quality.accuracy.threshold")
  
  // 数据生命周期配置
  val ODS_RETENTION_DAYS: Int = config.getInt("sales.lifecycle.ods.retention.days")
  val DWD_RETENTION_DAYS: Int = config.getInt("sales.lifecycle.dwd.retention.days")
  val DWS_DAY_RETENTION_DAYS: Int = config.getInt("sales.lifecycle.dws.day.retention.days")
  val DWS_MONTH_RETENTION_YEARS: Int = config.getInt("sales.lifecycle.dws.month.retention.years")
  
  // 数据处理窗口配置
  val DEFAULT_PROCESSING_WINDOW: String = config.getString("sales.processing.default.window")
  val MAX_RETRY_COUNT: Int = config.getInt("sales.processing.max.retry.count")
  val RETRY_INTERVAL_SECONDS: Int = config.getInt("sales.processing.retry.interval.seconds")
  
  // 数据一致性检查阈值
  val ODS_TO_DWD_CONSISTENCY_THRESHOLD: Double = config.getDouble("sales.consistency.ods_to_dwd.threshold")
  val SUMMARY_ERROR_THRESHOLD: Double = config.getDouble("sales.consistency.summary.error.threshold")
  
  // 数据库名称常量
  val ODS_DATABASE = "ods_sales"
  val DWD_DATABASE = "dwd_sales"
  val DWS_DATABASE = "dws_sales"
  val DQ_DATABASE = "dq_sales"
  
  // JDBC连接配置
  val MYSQL_DRIVER = "com.mysql.jdbc.Driver"
  val MYSQL_URL = "jdbc:mysql://localhost:3306/sales?useSSL=false"
  val MYSQL_USER = "root"
  val MYSQL_PASSWORD = "123456"
  
  // Doris连接配置
  val DORIS_DRIVER = "com.mysql.jdbc.Driver"
  val DORIS_HTTP_URL = "http://localhost:8030"
  
  // 数据保留配置
  val DATA_RETENTION_DAYS = 365  // 数据保留天数
  val PARTITION_RETENTION_DAYS = 30  // 分区保留天数（短期内的分区保留）
  
  // 数据处理并行度
  val MAX_PROCESSING_THREADS = 10  // 最大处理线程数
  
  // 文件路径配置
  val LOCAL_DATA_PATH = "data"
  val TMP_DATA_PATH = "tmp"
  val LOG_PATH = "log"
  
  /**
   * 从spark-defaults.conf或系统配置中加载配置
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 配置值
   */
  def getSparkConfig(spark: SparkSession, key: String, defaultValue: String): String = {
    Try(spark.conf.get(key)).getOrElse(defaultValue)
  }
  
  /**
   * 获取系统环境变量
   * @param key 环境变量名
   * @param defaultValue 默认值
   * @return 环境变量值
   */
  def getEnv(key: String, defaultValue: String): String = {
    Option(System.getenv(key)).getOrElse(defaultValue)
  }
  
  /**
   * 获取MySQL表配置
   * @param tableName 表名
   * @return (数据库名.表名, 主键字段)
   */
  def getMysqlTableConfig(tableName: String): (String, String) = {
    val tableConfigs = Map(
      "user" -> ("sales.user", "user_id"),
      "employee" -> ("sales.employee", "employee_id"),
      "product" -> ("sales.product", "product_id"),
      "sales_order" -> ("sales.sales_order", "order_id"),
      "sales_order_item" -> ("sales.sales_order_item", "order_item_id"),
      "purchase_order" -> ("sales.purchase_order", "purchase_id"),
      "purchase_order_item" -> ("sales.purchase_order_item", "purchase_item_id"),
      "supplier" -> ("sales.supplier", "supplier_id")
    )
    
    tableConfigs.getOrElse(tableName, (s"sales.$tableName", "id"))
  }
  
  /**
   * 获取数据质量检查配置
   * @return 数据质量检查配置列表
   */
  def getDataQualityChecks(): List[(String, String, String, String)] = {
    // (表名, 检查类型, 检查SQL, 严重级别)
    List(
      ("ods_user", "null_check", "SELECT COUNT(*) FROM ods_sales.ods_user WHERE user_id IS NULL OR user_name IS NULL", "high"),
      ("ods_product", "null_check", "SELECT COUNT(*) FROM ods_sales.ods_product WHERE product_id IS NULL OR product_name IS NULL", "high"),
      ("ods_sales_order", "null_check", "SELECT COUNT(*) FROM ods_sales.ods_sales_order WHERE order_id IS NULL OR user_id IS NULL", "high"),
      ("ods_sales_order_item", "null_check", "SELECT COUNT(*) FROM ods_sales.ods_sales_order_item WHERE order_item_id IS NULL OR order_id IS NULL OR product_id IS NULL", "high"),
      ("fact_sales_order", "integrity_check", "SELECT COUNT(*) FROM dwd_sales.fact_sales_order a LEFT JOIN dwd_sales.dim_user b ON a.user_id = b.user_id WHERE b.user_id IS NULL", "medium"),
      ("fact_sales_order_item", "integrity_check", "SELECT COUNT(*) FROM dwd_sales.fact_sales_order_item a LEFT JOIN dwd_sales.fact_sales_order b ON a.order_id = b.order_id WHERE b.order_id IS NULL", "medium"),
      ("fact_sales_order_item", "integrity_check", "SELECT COUNT(*) FROM dwd_sales.fact_sales_order_item a LEFT JOIN dwd_sales.dim_product b ON a.product_id = b.product_id WHERE b.product_id IS NULL", "medium"),
      ("dws_sales_day", "accuracy_check", "SELECT ABS(SUM(a.order_amount) - SUM(b.order_amount)) FROM dws_sales.dws_sales_day a JOIN dwd_sales.fact_sales_order b ON SUBSTR(b.order_date, 1, 10) = a.dt", "low")
    )
  }
  
  /**
   * 检查所有必需的配置是否已经设置
   * @return 如果所有必需的配置都已设置，则返回true，否则返回false
   */
  def validateRequiredConfigs(): Boolean = {
    val requiredConfigs = List(
      ("MYSQL_URL", MYSQL_URL),
      ("MYSQL_USER", MYSQL_USER),
      ("DORIS_JDBC_URL", DORIS_JDBC_URL),
      ("DORIS_USER", DORIS_USER)
    )
    
    val missingConfigs = requiredConfigs.filter { case (name, value) => 
      value == null || value.isEmpty
    }
    
    if (missingConfigs.nonEmpty) {
      logger.error("以下必需的配置未设置:")
      missingConfigs.foreach { case (name, _) => 
        logger.error(s"  - $name")
      }
      false
    } else {
      true
    }
  }
  
  /**
   * 获取MySQL连接属性
   * @return MySQL连接属性
   */
  def getMysqlConnectionProperties(): java.util.Properties = {
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", MYSQL_USER)
    connectionProperties.put("password", MYSQL_PASSWORD)
    connectionProperties.put("driver", MYSQL_DRIVER)
    connectionProperties
  }
  
  /**
   * 获取Doris连接属性
   * @return Doris连接属性
   */
  def getDorisConnectionProperties(): java.util.Properties = {
    val connectionProperties = new java.util.Properties()
    connectionProperties.put("user", DORIS_USER)
    connectionProperties.put("password", DORIS_PASSWORD)
    connectionProperties
  }
  
  // 配置文件路径
  private val CONFIG_FILE = "conf/application.properties"
  
  /**
   * 获取MySQL连接属性
   * @return MySQL连接属性
   */
  def getMySQLProperties(): Properties = {
    val props = new Properties()
    props.setProperty("user", MYSQL_USER)
    props.setProperty("password", MYSQL_PASSWORD)
    props.setProperty("driver", MYSQL_DRIVER)
    props
  }
  
  /**
   * 获取Doris连接属性
   * @return Doris连接属性
   */
  def getDorisProperties(): Properties = {
    val props = new Properties()
    props.setProperty("user", DORIS_USER)
    props.setProperty("password", DORIS_PASSWORD)
    props
  }
  
  /**
   * 确保数据库存在
   * @param spark SparkSession实例
   * @param databases 数据库名称列表
   */
  def ensureDatabasesExist(spark: SparkSession, databases: Seq[String]): Unit = {
    databases.foreach { db =>
      logger.info(s"确保数据库存在: $db")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $db")
    }
  }
  
  /**
   * 创建MySQL到JDBC读取选项
   * @param tableName MySQL表名
   * @param partitionColumn 分区列名
   * @param lowerBound 分区下界
   * @param upperBound 分区上界
   * @param numPartitions 分区数
   * @return JDBC读取选项
   */
  def createMySQLReadOptions(
    tableName: String, 
    partitionColumn: String = "", 
    lowerBound: Long = 0, 
    upperBound: Long = 1000000, 
    numPartitions: Int = 10
  ): Map[String, String] = {
    var options = Map(
      "url" -> MYSQL_URL,
      "dbtable" -> tableName,
      "user" -> MYSQL_USER,
      "password" -> MYSQL_PASSWORD,
      "driver" -> MYSQL_DRIVER
    )
    
    if (partitionColumn.nonEmpty) {
      options ++= Map(
        "partitionColumn" -> partitionColumn,
        "lowerBound" -> lowerBound.toString,
        "upperBound" -> upperBound.toString,
        "numPartitions" -> numPartitions.toString
      )
    }
    
    options
  }
  
  /**
   * 从配置文件读取属性
   * @param key 配置键
   * @param defaultValue 默认值
   * @return 属性值
   */
  private def getProperty(key: String, defaultValue: String): String = {
    Try {
      val properties = new Properties()
      val file = new FileInputStream(CONFIG_FILE)
      try {
        properties.load(file)
      } finally {
        file.close()
      }
      Option(properties.getProperty(key)).getOrElse(defaultValue)
    } match {
      case Success(value) => value
      case Failure(e) => 
        logger.warn(s"无法从配置文件读取属性 $key，使用默认值: $defaultValue, 原因: ${e.getMessage}")
        defaultValue
    }
  }
  
  /**
   * 获取所有配置项
   * @return 配置集合
   */
  def getAllConfigs(): Map[String, String] = {
    Try {
      val properties = new Properties()
      val file = new FileInputStream(CONFIG_FILE)
      try {
        properties.load(file)
        properties.asScala.toMap
      } finally {
        file.close()
      }
    } match {
      case Success(configs) => configs
      case Failure(e) => 
        logger.warn(s"无法读取配置文件，返回空配置: ${e.getMessage}")
        Map.empty[String, String]
    }
  }
} 