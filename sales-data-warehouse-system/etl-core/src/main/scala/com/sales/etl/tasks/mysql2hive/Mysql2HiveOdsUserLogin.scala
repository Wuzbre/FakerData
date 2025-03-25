package com.sales.etl.tasks.mysql2hive

import com.sales.etl.common.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object Mysql2HiveOdsUserLogin extends SparkSessionWrapper {
  def main(args: Array[String]): Unit = {
    try {
      // 读取MySQL数据
      val mysqlDF = spark.read
        .format("jdbc")
        .option("url", config.getString("mysql.source.url"))
        .option("driver", config.getString("mysql.source.driver"))
        .option("user", config.getString("mysql.source.user"))
        .option("password", config.getString("mysql.source.password"))
        .option("dbtable", "user_login_logs")
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
      col("log_id"),
      col("user_id"),
      col("login_time"),
      col("logout_time"),
      col("ip_address"),
      col("device_type"),
      col("os_type"),
      col("browser_type"),
      col("session_id"),
      col("login_status"),
      col("login_source"),
      split(col("location"), ",").getItem(0).as("province"),
      split(col("location"), ",").getItem(1).as("city"),
      col("created_at").as("create_time"),
      current_timestamp().as("etl_time"),
      date_format(col("login_time"), "yyyy-MM-dd").as("dt")
    )
  }

  def writeToHive(df: DataFrame): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .partitionBy("dt")
      .format("parquet")
      .saveAsTable("ods.ods_user_login_log")
  }

  def backupData(df: DataFrame): Unit = {
    val backupPath = s"${config.getString("etl.paths.archive")}/user_login_logs/${LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyyMMdd"))}"
    
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(backupPath)
  }
} 