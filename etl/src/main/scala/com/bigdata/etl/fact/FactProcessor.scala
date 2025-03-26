package com.bigdata.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * DWD层数据处理基类
 */
abstract class FactProcessor(spark: SparkSession) {
  
  /**
   * 获取当前处理的分区日期
   */
  protected def getProcessDate: String = {
    // 这里可以从配置文件或参数中获取，这里暂时写死
    "2023-12-31"
  }

  /**
   * 添加ETL处理时间和分区字段
   */
  protected def addETLColumns(df: DataFrame): DataFrame = {
    df.withColumn("etl_time", current_timestamp())
      .withColumn("dt", lit(getProcessDate))
  }

  /**
   * 处理数据的主方法
   */
  def process(): Unit

  /**
   * 保存数据到Hive表
   */
  protected def saveToHive(df: DataFrame, tableName: String, mode: String = "overwrite"): Unit = {
    df.write
      .format("hive")
      .mode(mode)
      .partitionBy("dt")
      .saveAsTable(tableName)
  }

  /**
   * 读取ODS层数据
   */
  protected def readFromODS(tableName: String): DataFrame = {
    spark.table(s"ods.$tableName")
  }

  /**
   * 读取DIM层数据
   */
  protected def readFromDIM(tableName: String): DataFrame = {
    spark.table(s"dim.$tableName")
  }
} 