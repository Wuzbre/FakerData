package com.sales.etl.tasks.hive2doris

import com.sales.etl.common.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.util.Properties
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Hive2DorisUserStats extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    try {
      // 读取Hive数据
      val userStatsDF = spark.sql("""
        SELECT 
          user_id,
          COUNT(DISTINCT order_id) as order_count,
          SUM(total_amount) as total_amount,
          AVG(total_amount) as avg_order_amount,
          MAX(order_date) as last_order_date,
          COUNT(DISTINCT product_id) as product_count,
          dt
        FROM ods.mysql2hive_ods_orders
        GROUP BY user_id, dt
      """)

      // 数据转换
      val transformedDF = transform(userStatsDF)
      
      // 写入Doris
      writeToDoris(transformedDF)
      
      // 备份数据
      backupData(transformedDF)
      
      spark.stop()
    } catch {
      case e: Exception =>
        println(s"ETL任务失败: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    }
  }

  def transform(df: DataFrame): DataFrame = {
    df.withColumn("etl_time", current_timestamp())
  }

  def writeToDoris(df: DataFrame): Unit = {
    val props = new Properties()
    props.setProperty("user", config.getString("doris.user"))
    props.setProperty("password", config.getString("doris.password"))
    
    df.write
      .mode(SaveMode.Append)
      .jdbc(
        config.getString("doris.url"),
        "sales_dw.hive2doris_user_stats",
        props
      )
  }

  def backupData(df: DataFrame): Unit = {
    val backupPath = s"${config.getString("etl.paths.archive")}/user_stats/${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}"
    
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(backupPath)
  }
} 