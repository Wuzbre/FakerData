package com.bigdata.etl.ads

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * ADS层数据处理基类
 */
abstract class AdsProcessor(spark: SparkSession) {
  
  // 处理日期
  protected var dt: String = _
  
  /**
   * 设置处理日期
   * @param date 日期，格式：yyyy-MM-dd
   */
  def setDate(date: String): this.type = {
    this.dt = date
    this
  }
  
  /**
   * 数据处理主方法
   */
  def process(): Unit = {
    try {
      // 1. 数据准备
      val data = prepareData()
      
      // 2. 数据转换
      val result = transform(data)
      
      // 3. 数据保存
      saveData(result)
      
    } catch {
      case e: Exception =>
        println(s"数据处理失败: ${e.getMessage}")
        e.printStackTrace()
        throw e
    }
  }
  
  /**
   * 准备数据
   * @return 数据集合
   */
  protected def prepareData(): Map[String, DataFrame]
  
  /**
   * 数据转换
   * @param data 输入数据
   * @return 转换后的数据
   */
  protected def transform(data: Map[String, DataFrame]): DataFrame
  
  /**
   * 保存数据
   * @param df 待保存的数据
   */
  protected def saveData(df: DataFrame): Unit = {
    // 添加ETL处理时间
    val resultDF = df.withColumn("etl_time", current_timestamp())
    
    // 保存数据
    resultDF.write
      .mode("overwrite")
      .format("hive")
      .partitionBy("dt")
      .saveAsTable(getTargetTable)
  }
  
  /**
   * 获取目标表名
   * @return 完整的表名（包含库名）
   */
  protected def getTargetTable: String
  
  /**
   * 计算环比增长率
   * @param df 数据集
   * @param valueCol 待计算的值列名
   * @return 环比增长率
   */
  protected def calculateMoM(df: DataFrame, valueCol: String): DataFrame = {
    val windowSpec = org.apache.spark.sql.expressions.Window
      .orderBy(col("dt"))
    
    df.withColumn("prev_value", lag(col(valueCol), 1).over(windowSpec))
      .withColumn("mom_rate", 
        when(col("prev_value").isNotNull && col("prev_value") =!= 0,
          (col(valueCol) - col("prev_value")) / col("prev_value")
        ).otherwise(lit(0))
      )
      .drop("prev_value")
  }
  
  /**
   * 计算同比增长率
   * @param df 数据集
   * @param valueCol 待计算的值列名
   * @return 同比增长率
   */
  protected def calculateYoY(df: DataFrame, valueCol: String): DataFrame = {
    val windowSpec = org.apache.spark.sql.expressions.Window
      .orderBy(col("dt"))
    
    df.withColumn("prev_year_value", lag(col(valueCol), 12).over(windowSpec))
      .withColumn("yoy_rate",
        when(col("prev_year_value").isNotNull && col("prev_year_value") =!= 0,
          (col(valueCol) - col("prev_year_value")) / col("prev_year_value")
        ).otherwise(lit(0))
      )
      .drop("prev_year_value")
  }
  
  /**
   * 计算排名
   * @param df 数据集
   * @param orderCol 排序列名
   * @param partitionCols 分区列名（可选）
   * @return 添加排名后的数据集
   */
  protected def calculateRank(
    df: DataFrame, 
    orderCol: String, 
    partitionCols: Seq[String] = Seq.empty
  ): DataFrame = {
    val windowSpec = if (partitionCols.isEmpty) {
      org.apache.spark.sql.expressions.Window
        .orderBy(col(orderCol).desc)
    } else {
      org.apache.spark.sql.expressions.Window
        .partitionBy(partitionCols.map(col): _*)
        .orderBy(col(orderCol).desc)
    }
    
    df.withColumn("rank_num", row_number().over(windowSpec))
  }
} 