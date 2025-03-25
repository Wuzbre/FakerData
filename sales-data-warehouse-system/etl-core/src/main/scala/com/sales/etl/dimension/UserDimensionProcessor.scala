package com.sales.etl.dimension

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class UserDimensionProcessor(spark: SparkSession) extends DimensionProcessor(spark) {
  override protected val dimensionName: String = "user"
  override protected val sourceTable: String = "ods_users"
  override protected val targetTable: String = "dwd_dim_users"
  
  override protected def loadSourceData(date: String): DataFrame = {
    spark.sql(s"""
      SELECT 
        id as user_id,
        name as user_name,
        email,
        phone,
        address,
        created_time,
        update_time
      FROM $sourceTable
      WHERE dt = '$date'
    """)
  }
  
  override protected def saveResults(df: DataFrame, date: String): Unit = {
    df.write
      .mode("overwrite")
      .partitionBy("dt")
      .format("parquet")
      .saveAsTable(targetTable)
  }
  
  override protected def createDimensionTable(): Unit = {
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $targetTable (
        user_key BIGINT,
        user_id BIGINT,
        user_name STRING,
        email STRING,
        phone STRING,
        address STRING,
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
} 