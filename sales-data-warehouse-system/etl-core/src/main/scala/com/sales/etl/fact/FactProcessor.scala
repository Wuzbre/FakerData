package com.sales.etl.fact

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

abstract class FactProcessor(spark: SparkSession) {
  protected val factName: String
  protected val sourceTable: String
  protected val targetTable: String
  
  // 获取事实表键列名
  protected def getFactKey: String = s"${factName}_key"
  
  // 创建事实表
  protected def createFactTable(): Unit
  
  // 加载维度表
  protected def loadDimensionTable(tableName: String): DataFrame = {
    spark.sql(s"""
      SELECT * FROM $tableName
      WHERE is_current = true
    """)
  }
  
  // 生成事实表键
  protected def generateFactKey(df: DataFrame): DataFrame = {
    df.withColumn(getFactKey, monotonically_increasing_id())
  }
  
  // 添加ETL处理时间
  protected def addETLTimestamp(df: DataFrame): DataFrame = {
    df.withColumn("etl_timestamp", current_timestamp())
  }
  
  // 处理事实数据
  def process(date: String): Unit = {
    // 1. 确保目标表存在
    createFactTable()
    
    // 2. 加载源数据
    val sourceDF = loadSourceData(date)
    
    // 3. 处理维度关联
    val processedDF = processDimensions(sourceDF)
    
    // 4. 添加事实表键和ETL时间戳
    val finalDF = generateFactKey(processedDF)
      .transform(addETLTimestamp)
    
    // 5. 保存结果
    saveResults(finalDF, date)
  }
  
  // 加载源数据
  protected def loadSourceData(date: String): DataFrame
  
  // 处理维度关联
  protected def processDimensions(df: DataFrame): DataFrame
  
  // 保存结果
  protected def saveResults(df: DataFrame, date: String): Unit = {
    df.write
      .mode("append")
      .partitionBy("dt")
      .format("parquet")
      .saveAsTable(targetTable)
  }
  
  // 数据质量检查
  protected def validateData(df: DataFrame): Boolean = {
    // 1. 检查空值
    val nullCheckPassed = checkNullValues(df)
    
    // 2. 检查重复值
    val duplicateCheckPassed = checkDuplicates(df)
    
    // 3. 检查维度关联完整性
    val dimensionIntegrityPassed = checkDimensionIntegrity(df)
    
    nullCheckPassed && duplicateCheckPassed && dimensionIntegrityPassed
  }
  
  // 检查空值
  protected def checkNullValues(df: DataFrame): Boolean = {
    val nullCounts = df.select(df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c)): _*).first()
    val totalRows = df.count()
    
    df.columns.forall { c =>
      val nullCount = nullCounts.getAs[Long](c)
      val nullRatio = nullCount.toDouble / totalRows
      nullRatio <= 0.1 // 允许最多10%的空值
    }
  }
  
  // 检查重复值
  protected def checkDuplicates(df: DataFrame): Boolean = {
    val totalRows = df.count()
    val distinctRows = df.distinct().count()
    
    (totalRows - distinctRows).toDouble / totalRows <= 0.01 // 允许最多1%的重复
  }
  
  // 检查维度关联完整性
  protected def checkDimensionIntegrity(df: DataFrame): Boolean = {
    // 由子类实现具体的维度关联检查逻辑
    true
  }
  
  // 计算聚合指标
  protected def calculateMetrics(df: DataFrame): DataFrame = {
    // 由子类实现具体的指标计算逻辑
    df
  }
} 