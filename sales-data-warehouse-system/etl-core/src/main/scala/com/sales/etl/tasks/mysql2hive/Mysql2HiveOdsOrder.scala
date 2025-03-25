package com.sales.etl.tasks.mysql2hive

import com.sales.etl.common.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Mysql2HiveOdsOrder extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    try {
      // 读取MySQL数据
      val mysqlDF = spark.read
        .format("jdbc")
        .option("url", config.getString("mysql.source.url"))
        .option("driver", config.getString("mysql.source.driver"))
        .option("user", config.getString("mysql.source.user"))
        .option("password", config.getString("mysql.source.password"))
        .option("dbtable", "orders")
        .load()

      // 数据转换
      val transformedDF = transform(mysqlDF)
      
      // 写入Hive
      writeToHive(transformedDF)
      
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
    df.select(
      col("order_id"),
      col("user_id"),
      col("order_status"),
      col("payment_status"),
      col("shipping_status"),
      col("address_id"),
      col("coupon_id"),
      col("order_amount"),
      col("discount_amount"),
      col("shipping_amount"),
      col("payment_amount"),
      col("shipping_company"),
      col("shipping_sn"),
      col("payment_time"),
      col("shipping_time"),
      col("receive_time"),
      col("order_comment"),
      col("create_time"),
      col("update_time"),
      current_timestamp().as("etl_time"),
      date_format(col("create_time"), "yyyy-MM-dd").as("dt")
    )
  }

  def writeToHive(df: DataFrame): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .format("parquet")
      .saveAsTable("ods.mysql2hive_ods_order_df")
  }

  def backupData(df: DataFrame): Unit = {
    val backupPath = s"${config.getString("etl.paths.archive")}/orders/${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}"
    
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(backupPath)
  }
} 