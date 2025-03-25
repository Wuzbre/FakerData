package com.sales.etl.dimension

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import java.time.LocalDate

abstract class DimensionProcessor(spark: SparkSession) {
  protected val dimensionName: String
  protected val sourceTable: String
  protected val targetTable: String
  
  // SCD (Slowly Changing Dimension) 类型
  protected val scdType: Int = 2 // 默认使用SCD2
  
  // 获取维度键列名
  protected def getDimensionKey: String = s"${dimensionName}_key"
  
  // 获取业务键列名
  protected def getBusinessKey: String = s"${dimensionName}_id"
  
  // 获取当前有效记录
  protected def getCurrentRecords: DataFrame = {
    spark.sql(s"""
      SELECT * FROM $targetTable
      WHERE is_current = true
    """)
  }
  
  // 处理SCD1（覆盖更新）
  protected def processSCD1(sourceDF: DataFrame, targetDF: DataFrame): DataFrame = {
    val updatedDF = sourceDF
      .join(targetDF, getBusinessKey, "left")
      .select(
        sourceDF("*"),
        targetDF(getDimensionKey)
      )
    
    updatedDF
  }
  
  // 处理SCD2（保留历史）
  protected def processSCD2(sourceDF: DataFrame, targetDF: DataFrame): DataFrame = {
    val now = LocalDate.now()
    
    // 1. 找出发生变化的记录
    val changedRecords = sourceDF
      .join(targetDF, getBusinessKey, "left")
      .where("hash_value <> target_hash_value")
    
    // 2. 将当前记录标记为历史记录
    val historicalRecords = targetDF
      .where(s"$getBusinessKey IN (SELECT $getBusinessKey FROM changedRecords)")
      .withColumn("end_date", lit(now))
      .withColumn("is_current", lit(false))
    
    // 3. 插入新记录
    val newRecords = changedRecords
      .withColumn("start_date", lit(now))
      .withColumn("end_date", lit(null))
      .withColumn("is_current", lit(true))
    
    // 4. 合并结果
    historicalRecords.union(newRecords)
  }
  
  // 生成维度表的哈希值
  protected def generateHashValue(df: DataFrame, excludeCols: Seq[String]): DataFrame = {
    val columns = df.columns.filterNot(excludeCols.contains)
    df.withColumn("hash_value", sha2(concat_ws("|", columns.map(col): _*), 256))
  }
  
  // 处理新增维度
  protected def processNewDimensions(sourceDF: DataFrame, targetDF: DataFrame): DataFrame = {
    val newRecords = sourceDF
      .join(targetDF, getBusinessKey, "left_anti")
      .withColumn(getDimensionKey, monotonically_increasing_id())
      .withColumn("start_date", current_date())
      .withColumn("end_date", lit(null))
      .withColumn("is_current", lit(true))
    
    newRecords
  }
  
  // 维度表基本结构
  protected def createDimensionTable(): Unit = {
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $targetTable (
        ${getDimensionKey} BIGINT,
        ${getBusinessKey} STRING,
        name STRING,
        description STRING,
        start_date DATE,
        end_date DATE,
        is_current BOOLEAN,
        created_time TIMESTAMP,
        updated_time TIMESTAMP,
        hash_value STRING
      )
      PARTITIONED BY (dt STRING)
      STORED AS PARQUET
    """)
  }
  
  // 执行维度处理
  def process(date: String): Unit = {
    // 1. 确保目标表存在
    createDimensionTable()
    
    // 2. 读取源数据
    val sourceDF = loadSourceData(date)
    
    // 3. 读取当前维度数据
    val targetDF = getCurrentRecords
    
    // 4. 根据SCD类型处理数据
    val processedDF = scdType match {
      case 1 => processSCD1(sourceDF, targetDF)
      case 2 => processSCD2(sourceDF, targetDF)
      case _ => throw new IllegalArgumentException(s"Unsupported SCD type: $scdType")
    }
    
    // 5. 保存结果
    saveResults(processedDF, date)
  }
  
  // 加载源数据（子类实现具体逻辑）
  protected def loadSourceData(date: String): DataFrame
  
  // 保存处理结果（子类实现具体逻辑）
  protected def saveResults(df: DataFrame, date: String): Unit
} 