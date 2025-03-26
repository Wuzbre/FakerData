package com.bigdata.etl.ads

import org.apache.spark.sql.SparkSession

/**
 * ADS层数据处理主程序
 */
object AdsApp {
  
  def main(args: Array[String]): Unit = {
    // 1. 检查参数
    if (args.length < 1) {
      println("请指定数据处理日期，格式：yyyy-MM-dd")
      System.exit(1)
    }
    val dt = args(0)
    
    // 2. 创建SparkSession
    val spark = SparkSession.builder()
      .appName("ADS Data Processing")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // 3. 处理用户分析主题
      processUserAnalysis(spark, dt)
      
      // 4. 处理商品分析主题
      processProductAnalysis(spark, dt)
      
      // 5. 处理供应商分析主题
      processSupplierAnalysis(spark, dt)
      
      // 6. 处理促销分析主题
      processPromotionAnalysis(spark, dt)
      
      // 7. 处理库存分析主题
      processInventoryAnalysis(spark, dt)
      
      // 8. 处理运营分析主题
      processOperationAnalysis(spark, dt)
      
      println("ADS层数据处理完成")
      
    } catch {
      case e: Exception =>
        println(s"ADS层数据处理失败: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
        
    } finally {
      spark.stop()
    }
  }
  
  /**
   * 处理用户分析主题
   */
  private def processUserAnalysis(spark: SparkSession, dt: String): Unit = {
    // 1. 用户价值分析
    println("开始处理用户价值分析...")
    new UserValueProcessor(spark)
      .setDate(dt)
      .process()
    
    // 2. 用户促销敏感度分析
    println("开始处理用户促销敏感度分析...")
    new UserPromoSensitivityProcessor(spark)
      .setDate(dt)
      .process()
    
    // 3. 用户消费趋势分析
    println("开始处理用户消费趋势分析...")
    new UserConsumptionTrendProcessor(spark)
      .setDate(dt)
      .process()
  }
  
  /**
   * 处理商品分析主题
   */
  private def processProductAnalysis(spark: SparkSession, dt: String): Unit = {
    // 1. 热销商品排行分析
    println("开始处理热销商品排行分析...")
    new HotProductRankProcessor(spark)
      .setDate(dt)
      .process()
    
    // 2. 商品类目销售分析
    println("开始处理商品类目销售分析...")
    new CategorySalesProcessor(spark)
      .setDate(dt)
      .process()
  }
  
  /**
   * 处理供应商分析主题
   */
  private def processSupplierAnalysis(spark: SparkSession, dt: String): Unit = {
    // 1. 供应商评分分析
    println("开始处理供应商评分分析...")
    new SupplierScoreProcessor(spark)
      .setDate(dt)
      .process()
    
    // 2. 供应商交付分析
    println("开始处理供应商交付分析...")
    new SupplierDeliveryProcessor(spark)
      .setDate(dt)
      .process()
  }
  
  /**
   * 处理促销分析主题
   */
  private def processPromotionAnalysis(spark: SparkSession, dt: String): Unit = {
    // 1. 促销活动效果排行分析
    println("开始处理促销活动效果排行分析...")
    new PromotionEffectProcessor(spark)
      .setDate(dt)
      .process()
    
    // 2. 促销活动客户分析
    println("开始处理促销活动客户分析...")
    new PromotionCustomerProcessor(spark)
      .setDate(dt)
      .process()
  }
  
  /**
   * 处理库存分析主题
   */
  private def processInventoryAnalysis(spark: SparkSession, dt: String): Unit = {
    // 1. 库存健康状况分析
    println("开始处理库存健康状况分析...")
    new InventoryHealthProcessor(spark)
      .setDate(dt)
      .process()
    
    // 2. 库存周转分析
    println("开始处理库存周转分析...")
    new InventoryTurnoverProcessor(spark)
      .setDate(dt)
      .process()
  }
  
  /**
   * 处理运营分析主题
   */
  private def processOperationAnalysis(spark: SparkSession, dt: String): Unit = {
    // 1. 整体运营指标分析
    println("开始处理整体运营指标分析...")
    new OperationOverallProcessor(spark)
      .setDate(dt)
      .process()
    
    // 2. 运营趋势分析
    println("开始处理运营趋势分析...")
    new OperationTrendProcessor(spark)
      .setDate(dt)
      .process()
  }
} 