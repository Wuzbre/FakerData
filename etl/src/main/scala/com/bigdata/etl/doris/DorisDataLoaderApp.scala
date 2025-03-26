package com.bigdata.etl.doris

import org.apache.spark.sql.SparkSession

/**
 * Doris数据加载器主程序
 */
object DorisDataLoaderApp {
  
  def main(args: Array[String]): Unit = {
    // 1. 检查参数
    if (args.length < 1) {
      println("请指定数据处理日期，格式：yyyy-MM-dd")
      System.exit(1)
    }
    val dt = args(0)
    
    // 2. 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Doris Data Loader")
      .enableHiveSupport()
      .config("spark.sql.broadcastTimeout", "3600")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate()
    
    try {
      println(s"开始将数据导入Doris，处理日期: $dt")
      
      val loader = new DorisDataLoader(spark)
      
      // 3. 加载DWS层数据
      println("开始加载DWS层数据...")
      loader.loadDwsData(dt)
      
      // 4. 加载ADS层数据
      println("开始加载ADS层数据...")
      loader.loadAdsData(dt)
      
      println("数据导入Doris完成")
      
    } catch {
      case e: Exception =>
        println(s"数据导入Doris失败: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
        
    } finally {
      spark.stop()
    }
  }
} 