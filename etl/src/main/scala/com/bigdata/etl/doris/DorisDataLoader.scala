package com.bigdata.etl.doris

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Doris数据加载器
 * 负责将Hive中的ADS和DWS层数据导入到Doris中
 */
class DorisDataLoader(spark: SparkSession) {
  
  // Doris连接配置
  private val DORIS_FENODES = "your_doris_fe_host:8030"
  private val DORIS_USER = "root"
  private val DORIS_PASSWORD = "your_password"
  private val DORIS_DATABASE = "retail_analysis"
  
  /**
   * 配置Doris写入属性
   */
  private def getDorisOptions(tableName: String): Map[String, String] = {
    Map(
      "doris.fenodes" -> DORIS_FENODES,
      "doris.table.identifier" -> s"${DORIS_DATABASE}.$tableName",
      "user" -> DORIS_USER,
      "password" -> DORIS_PASSWORD,
      "doris.batch.size" -> "10000",
      "doris.request.retries" -> "3",
      "doris.request.connect.timeout.ms" -> "30000",
      "doris.request.read.timeout.ms" -> "30000"
    )
  }
  
  /**
   * 加载ADS层数据到Doris
   */
  def loadAdsData(dt: String): Unit = {
    // 1. 用户分析相关表
    loadTableToDoris("ads_user_value", s"SELECT * FROM ads.ads_user_value_d WHERE dt = '$dt'")
    loadTableToDoris("ads_user_promo_sensitivity", s"SELECT * FROM ads.ads_user_promo_sensitivity_d WHERE dt = '$dt'")
    loadTableToDoris("ads_user_consumption_trend", s"SELECT * FROM ads.ads_user_consumption_trend_m WHERE substr(dt,1,7) = substr('$dt',1,7)")
    
    // 2. 商品分析相关表
    loadTableToDoris("ads_hot_product_rank", s"SELECT * FROM ads.ads_hot_product_rank_d WHERE dt = '$dt'")
    loadTableToDoris("ads_category_sales", s"SELECT * FROM ads.ads_category_sales_d WHERE dt = '$dt'")
    
    // 3. 供应商分析相关表
    loadTableToDoris("ads_supplier_score", s"SELECT * FROM ads.ads_supplier_score_d WHERE dt = '$dt'")
    loadTableToDoris("ads_supplier_delivery", s"SELECT * FROM ads.ads_supplier_delivery_d WHERE dt = '$dt'")
    
    // 4. 促销分析相关表
    loadTableToDoris("ads_promotion_effect", s"SELECT * FROM ads.ads_promotion_effect_d WHERE dt = '$dt'")
    loadTableToDoris("ads_promotion_customer", s"SELECT * FROM ads.ads_promotion_customer_d WHERE dt = '$dt'")
    
    // 5. 库存分析相关表
    loadTableToDoris("ads_inventory_health", s"SELECT * FROM ads.ads_inventory_health_d WHERE dt = '$dt'")
    loadTableToDoris("ads_inventory_turnover_stats", s"SELECT * FROM ads.ads_inventory_turnover_stats_m WHERE substr(dt,1,7) = substr('$dt',1,7)")
    
    // 6. 运营分析相关表
    loadTableToDoris("ads_operation_overall_stats", s"SELECT * FROM ads.ads_operation_overall_stats_d WHERE dt = '$dt'")
    loadTableToDoris("ads_operation_trend", s"SELECT * FROM ads.ads_operation_trend_m WHERE substr(dt,1,7) = substr('$dt',1,7)")
  }
  
  /**
   * 加载DWS层数据到Doris
   */
  def loadDwsData(dt: String): Unit = {
    // 1. 用户行为汇总表
    loadTableToDoris("dws_user_behavior", s"SELECT * FROM dws.dws_user_behavior WHERE dt = '$dt'")
    
    // 2. 订单汇总表
    loadTableToDoris("dws_order", s"SELECT * FROM dws.dws_order WHERE dt = '$dt'")
    
    // 3. 库存汇总表
    loadTableToDoris("dws_inventory", s"SELECT * FROM dws.dws_inventory WHERE dt = '$dt'")
    
    // 4. 供应商表现汇总表
    loadTableToDoris("dws_supplier_performance", s"SELECT * FROM dws.dws_supplier_performance WHERE dt = '$dt'")
  }
  
  /**
   * 将数据表加载到Doris
   */
  private def loadTableToDoris(tableName: String, query: String): Unit = {
    try {
      println(s"开始加载表 $tableName 到Doris...")
      
      val df = spark.sql(query)
      
      // 写入Doris
      df.write
        .format("doris")
        .options(getDorisOptions(tableName))
        .mode("append")
        .save()
      
      println(s"表 $tableName 加载完成")
    } catch {
      case e: Exception =>
        println(s"加载表 $tableName 失败: ${e.getMessage}")
        e.printStackTrace()
    }
  }
} 