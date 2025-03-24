package com.sales.utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

/**
 * SparkSession工具类，提供获取SparkSession实例的方法
 */
object SparkSessionUtils {
  
  private val config: Config = ConfigFactory.load()
  
  /**
   * 创建并配置SparkSession
   *
   * @param enableHive 是否启用Hive支持
   * @return SparkSession实例
   */
  def getSparkSession(enableHive: Boolean = true): SparkSession = {
    val sparkMaster = config.getString("sales.spark.master")
    val appName = config.getString("sales.spark.app.name")
    val warehouseDir = config.getString("sales.spark.sql.warehouse.dir")
    val metastoreUris = config.getString("sales.hive.metastore.uris")
    
    val sparkBuilder = SparkSession.builder()
      .appName(appName)
      
    // 如果是本地模式则设置master
    if (sparkMaster.startsWith("local")) {
      sparkBuilder.master(sparkMaster)
    }
    
    // 配置Spark参数
    sparkBuilder
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.executor.memory", config.getString("sales.spark.executor.memory"))
      .config("spark.executor.cores", config.getInt("sales.spark.executor.cores"))
      .config("spark.executor.instances", config.getInt("sales.spark.executor.instances"))
      .config("spark.driver.memory", config.getString("sales.spark.driver.memory"))
      .config("spark.sql.shuffle.partitions", config.getInt("sales.spark.shuffle.partitions"))
    
    // 启用Hive支持
    val sparkSession = if (enableHive) {
      sparkBuilder
        .config("hive.metastore.uris", metastoreUris)
        .enableHiveSupport()
        .getOrCreate()
    } else {
      sparkBuilder.getOrCreate()
    }
    
    // 设置日志级别
    sparkSession.sparkContext.setLogLevel("WARN")
    
    sparkSession
  }
  
  /**
   * 关闭SparkSession
   *
   * @param spark SparkSession实例
   */
  def closeSparkSession(spark: SparkSession): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }
} 