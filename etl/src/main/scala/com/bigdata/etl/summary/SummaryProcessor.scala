package com.bigdata.etl.summary

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * DWS层数据处理基类
 */
abstract class SummaryProcessor(spark: SparkSession) {
  
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
   * 读取DWD层数据
   */
  protected def readFromDWD(tableName: String): DataFrame = {
    spark.table(s"dwd.$tableName")
  }

  /**
   * 读取DIM层数据
   */
  protected def readFromDIM(tableName: String): DataFrame = {
    spark.table(s"dim.$tableName")
  }

  /**
   * 读取DWS层数据
   */
  protected def readFromDWS(tableName: String): DataFrame = {
    spark.table(s"dws.$tableName")
  }

  /**
   * 计算同环比
   * @param df 输入数据框
   * @param partitionCols 分组字段
   * @param targetCol 目标计算字段
   * @param dateCol 日期字段
   * @return 添加了环比和同比的数据框
   */
  protected def calculateGrowthRate(
    df: DataFrame, 
    partitionCols: Seq[String], 
    targetCol: String,
    dateCol: String = "dt"
  ): DataFrame = {
    val windowSpec = Window.partitionBy(partitionCols: _*)
      .orderBy(col(dateCol))

    df.withColumn("prev_value", 
        lag(col(targetCol), 1).over(windowSpec))
      .withColumn("prev_year_value",
        lag(col(targetCol), 365).over(windowSpec))
      .withColumn("mom_rate",  // 环比增长率
        when(col("prev_value").isNotNull,
          (col(targetCol) - col("prev_value")) / col("prev_value"))
        .otherwise(lit(0)))
      .withColumn("yoy_rate",  // 同比增长率
        when(col("prev_year_value").isNotNull,
          (col(targetCol) - col("prev_year_value")) / col("prev_year_value"))
        .otherwise(lit(0)))
  }
} 