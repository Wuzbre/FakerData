package com.sales.etl.dimension

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ProductDimensionProcessor(spark: SparkSession) extends DimensionProcessor(spark) {
  override protected val dimensionName: String = "product"
  override protected val sourceTable: String = "ods_products"
  override protected val targetTable: String = "dwd_dim_products"
  
  override protected def loadSourceData(date: String): DataFrame = {
    spark.sql(s"""
      SELECT 
        id as product_id,
        name as product_name,
        category,
        sub_category,
        brand,
        price,
        cost,
        description,
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
        product_key BIGINT,
        product_id BIGINT,
        product_name STRING,
        category STRING,
        sub_category STRING,
        brand STRING,
        price DECIMAL(10,2),
        cost DECIMAL(10,2),
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
} 