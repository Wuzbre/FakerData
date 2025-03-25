package com.sales.etl.common

import org.apache.spark.sql.SparkSession
import com.typesafe.config.ConfigFactory

trait SparkSessionWrapper {
  lazy val config = ConfigFactory.load()
  
  lazy val spark: SparkSession = SparkSession.builder()
    .appName(config.getString("spark.app.name"))
    .master(config.getString("spark.master"))
    .config("spark.executor.memory", config.getString("spark.executor.memory"))
    .config("spark.executor.cores", config.getString("spark.executor.cores"))
    .config("spark.default.parallelism", config.getString("spark.default.parallelism"))
    .config("hive.metastore.uris", config.getString("hive.metastore"))
    .enableHiveSupport()
    .getOrCreate()
} 