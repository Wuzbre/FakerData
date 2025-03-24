package com.sales.analysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.sales.utils.{ConfigUtils, SparkSessionUtils, DateUtils}
import org.slf4j.LoggerFactory

/**
 * 销售数据分析主类
 * 
 * 通过Spark SQL分析销售、采购等多维度数据
 * 将分析结果保存到Doris中，供前端可视化展示
 */
object SalesAnalysis {
  private val logger = LoggerFactory.getLogger(this.getClass)
  
  def main(args: Array[String]): Unit = {
    logger.info("开始执行销售数据分析...")
    
    // 初始化Spark会话
    val spark = SparkSessionUtils.getSparkSession()
    val config = ConfigUtils.getConfig()
    val etlDate = DateUtils.getEtlDate(args)
    
    try {
      // 注册临时视图，简化查询
      registerTempViews(spark, etlDate)
      
      // 1. 销售分析
      logger.info("分析销售趋势...")
      val salesTrend = analyzeSalesTrend(spark, etlDate)
      saveToDoris(salesTrend, "sales_trend_analysis")
      
      logger.info("分析热销产品...")
      val topProducts = analyzeTopProducts(spark, etlDate)
      saveToDoris(topProducts, "top_products_analysis")
      
      logger.info("分析客户RFM指标...")
      val customerRFM = analyzeCustomerRFM(spark, etlDate)
      saveToDoris(customerRFM, "customer_rfm_analysis")
      
      logger.info("分析销售地域分布...")
      val regionSales = analyzeRegionSales(spark, etlDate)
      saveToDoris(regionSales, "region_sales_analysis")
      
      // 2. 采购分析
      logger.info("分析供应商采购情况...")
      val supplierAnalysis = analyzeSupplierPerformance(spark, etlDate)
      saveToDoris(supplierAnalysis, "supplier_performance_analysis")
      
      logger.info("分析采购与销售关联...")
      val purchaseSalesAnalysis = analyzePurchaseSalesRelation(spark, etlDate)
      saveToDoris(purchaseSalesAnalysis, "purchase_sales_analysis")
      
      // 3. 库存分析
      logger.info("分析库存周转率...")
      val inventoryTurnover = analyzeInventoryTurnover(spark, etlDate)
      saveToDoris(inventoryTurnover, "inventory_turnover_analysis")
      
      // 4. 业务综合仪表盘数据
      logger.info("生成业务综合指标...")
      val businessDashboard = generateBusinessDashboard(spark, etlDate)
      saveToDoris(businessDashboard, "business_dashboard")
      
      logger.info("数据分析执行完成！")
    } catch {
      case e: Exception => 
        logger.error(s"数据分析过程发生错误: ${e.getMessage}", e)
        throw e
    } finally {
      spark.stop()
    }
  }
  
  /**
   * 注册临时视图
   */
  def registerTempViews(spark: SparkSession, etlDate: String): Unit = {
    val dwdDb = ConfigUtils.getHiveDwdDb()
    val dwsDb = ConfigUtils.getHiveDwsDb()
    
    // 注册维度表视图
    spark.sql(s"SELECT * FROM $dwdDb.dwd_dim_user WHERE dt='$etlDate'").createOrReplaceTempView("dim_user")
    spark.sql(s"SELECT * FROM $dwdDb.dwd_dim_product WHERE dt='$etlDate'").createOrReplaceTempView("dim_product")
    spark.sql(s"SELECT * FROM $dwdDb.dwd_dim_employee WHERE dt='$etlDate'").createOrReplaceTempView("dim_employee")
    spark.sql(s"SELECT * FROM $dwdDb.dwd_dim_supplier WHERE dt='$etlDate'").createOrReplaceTempView("dim_supplier")
    
    // 注册事实表视图
    spark.sql(s"SELECT * FROM $dwdDb.dwd_fact_sales_order WHERE dt='$etlDate'").createOrReplaceTempView("fact_sales_order")
    spark.sql(s"SELECT * FROM $dwdDb.dwd_fact_sales_order_item WHERE dt='$etlDate'").createOrReplaceTempView("fact_sales_order_item")
    spark.sql(s"SELECT * FROM $dwdDb.dwd_fact_purchase_order WHERE dt='$etlDate'").createOrReplaceTempView("fact_purchase_order")
    spark.sql(s"SELECT * FROM $dwdDb.dwd_fact_purchase_order_item WHERE dt='$etlDate'").createOrReplaceTempView("fact_purchase_order_item")
    
    // 注册汇总表视图
    spark.sql(s"SELECT * FROM $dwsDb.dws_sales_day_agg WHERE dt='$etlDate'").createOrReplaceTempView("dws_sales_day_agg")
    spark.sql(s"SELECT * FROM $dwsDb.dws_product_sales WHERE dt='$etlDate'").createOrReplaceTempView("dws_product_sales")
    spark.sql(s"SELECT * FROM $dwsDb.dws_customer_behavior WHERE dt='$etlDate'").createOrReplaceTempView("dws_customer_behavior")
    spark.sql(s"SELECT * FROM $dwsDb.dws_supplier_purchase WHERE dt='$etlDate'").createOrReplaceTempView("dws_supplier_purchase")
  }
  
  /**
   * 分析销售趋势
   * - 按日/周/月的销售额和订单量走势
   */
  def analyzeSalesTrend(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT 
        |  date_format(order_date, 'yyyy-MM-dd') as order_day,
        |  date_format(order_date, 'yyyy-MM') as order_month,
        |  date_format(order_date, 'yyyy-W') as order_week,
        |  count(order_id) as order_count,
        |  sum(total_amount) as sales_amount,
        |  count(distinct user_id) as customer_count
        |FROM fact_sales_order
        |GROUP BY 
        |  date_format(order_date, 'yyyy-MM-dd'),
        |  date_format(order_date, 'yyyy-MM'),
        |  date_format(order_date, 'yyyy-W')
        |ORDER BY order_day
      """.stripMargin)
  }
  
  /**
   * 分析热销产品
   * - 产品销售额/销售量排行
   */
  def analyzeTopProducts(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT 
        |  p.product_id,
        |  p.product_name,
        |  p.category,
        |  sum(i.quantity) as total_quantity,
        |  sum(i.amount) as total_amount,
        |  count(distinct i.order_id) as order_count,
        |  avg(i.unit_price) as avg_price
        |FROM fact_sales_order_item i
        |JOIN dim_product p ON i.product_id = p.product_id
        |GROUP BY p.product_id, p.product_name, p.category
        |ORDER BY total_amount DESC
      """.stripMargin)
  }
  
  /**
   * 客户RFM分析
   * - 分析客户价值
   */
  def analyzeCustomerRFM(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT 
        |  u.user_id,
        |  u.user_name,
        |  u.user_level,
        |  u.register_date,
        |  u.region,
        |  -- Recency: 最近一次购买时间距今天数
        |  datediff(current_date(), max(o.order_date)) as recency,
        |  -- Frequency: 购买频率
        |  count(distinct o.order_id) as frequency,
        |  -- Monetary: 购买金额
        |  sum(o.total_amount) as monetary,
        |  -- 额外指标
        |  min(o.order_date) as first_order_date,
        |  max(o.order_date) as last_order_date,
        |  avg(o.total_amount) as avg_order_amount
        |FROM fact_sales_order o
        |JOIN dim_user u ON o.user_id = u.user_id
        |GROUP BY u.user_id, u.user_name, u.user_level, u.register_date, u.region
      """.stripMargin)
  }
  
  /**
   * 销售地域分布分析
   */
  def analyzeRegionSales(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT 
        |  u.region,
        |  count(distinct o.order_id) as order_count,
        |  count(distinct o.user_id) as customer_count,
        |  sum(o.total_amount) as total_sales,
        |  avg(o.total_amount) as avg_order_amount
        |FROM fact_sales_order o
        |JOIN dim_user u ON o.user_id = u.user_id
        |GROUP BY u.region
        |ORDER BY total_sales DESC
      """.stripMargin)
  }
  
  /**
   * 供应商绩效分析
   */
  def analyzeSupplierPerformance(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT 
        |  s.supplier_id,
        |  s.supplier_name,
        |  s.contact_name,
        |  s.contact_phone,
        |  count(distinct p.order_id) as order_count,
        |  sum(p.total_amount) as purchase_amount,
        |  avg(p.total_amount) as avg_order_amount,
        |  -- 计算平均交货周期（天）
        |  avg(datediff(p.delivery_date, p.order_date)) as avg_delivery_days,
        |  -- 按时交货率
        |  sum(case when p.delivery_date <= p.expected_date then 1 else 0 end) / count(p.order_id) as on_time_delivery_rate
        |FROM fact_purchase_order p
        |JOIN dim_supplier s ON p.supplier_id = s.supplier_id
        |GROUP BY s.supplier_id, s.supplier_name, s.contact_name, s.contact_phone
        |ORDER BY purchase_amount DESC
      """.stripMargin)
  }
  
  /**
   * 分析采购与销售关联
   * - 产品采购成本与销售利润率
   */
  def analyzePurchaseSalesRelation(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT 
        |  p.product_id,
        |  p.product_name,
        |  p.category,
        |  -- 采购指标
        |  sum(pi.quantity) as purchase_quantity,
        |  sum(pi.quantity * pi.unit_price) as purchase_cost,
        |  -- 销售指标
        |  sum(si.quantity) as sales_quantity,
        |  sum(si.quantity * si.unit_price) as sales_revenue,
        |  -- 利润指标
        |  sum(si.quantity * si.unit_price) - sum(pi.quantity * pi.unit_price) as gross_profit,
        |  (sum(si.quantity * si.unit_price) - sum(pi.quantity * pi.unit_price)) / 
        |   nullif(sum(pi.quantity * pi.unit_price), 0) * 100 as profit_margin,
        |  -- 库存周转率相关
        |  sum(si.quantity) / nullif(sum(pi.quantity) - sum(si.quantity), 0) as inventory_turnover_rate
        |FROM fact_purchase_order_item pi
        |JOIN fact_sales_order_item si ON pi.product_id = si.product_id
        |JOIN dim_product p ON pi.product_id = p.product_id
        |GROUP BY p.product_id, p.product_name, p.category
        |ORDER BY profit_margin DESC
      """.stripMargin)
  }
  
  /**
   * 分析库存周转率
   */
  def analyzeInventoryTurnover(spark: SparkSession, etlDate: String): DataFrame = {
    // 假设我们有一个ods_inventory表
    // 如果没有，您可能需要通过采购量-销售量推算库存或者创建一个模拟的库存表
    spark.sql(
      """
        |SELECT 
        |  p.product_id,
        |  p.product_name,
        |  p.category,
        |  -- 销售指标
        |  sum(si.quantity) as sales_quantity,
        |  sum(si.amount) as sales_amount,
        |  -- 库存指标 (使用采购-销售作为近似库存值)
        |  sum(pi.quantity) - sum(si.quantity) as estimated_inventory,
        |  -- 周转率计算
        |  case 
        |    when (sum(pi.quantity) - sum(si.quantity)) > 0 
        |    then sum(si.quantity) / (sum(pi.quantity) - sum(si.quantity))
        |    else 0
        |  end as turnover_rate,
        |  -- 周转天数
        |  case 
        |    when sum(si.quantity) > 0 
        |    then (sum(pi.quantity) - sum(si.quantity)) / (sum(si.quantity) / 30)
        |    else 0
        |  end as turnover_days
        |FROM fact_sales_order_item si
        |JOIN fact_purchase_order_item pi ON si.product_id = pi.product_id
        |JOIN dim_product p ON si.product_id = p.product_id
        |GROUP BY p.product_id, p.product_name, p.category
      """.stripMargin)
  }
  
  /**
   * 生成业务综合仪表盘数据
   */
  def generateBusinessDashboard(spark: SparkSession, etlDate: String): DataFrame = {
    spark.sql(
      """
        |SELECT
        |  current_date() as report_date,
        |  -- 销售指标
        |  (SELECT count(distinct order_id) FROM fact_sales_order) as total_sales_orders,
        |  (SELECT sum(total_amount) FROM fact_sales_order) as total_sales_amount,
        |  (SELECT count(distinct user_id) FROM fact_sales_order) as total_customers,
        |  (SELECT avg(total_amount) FROM fact_sales_order) as avg_order_value,
        |  -- 采购指标
        |  (SELECT count(distinct order_id) FROM fact_purchase_order) as total_purchase_orders,
        |  (SELECT sum(total_amount) FROM fact_purchase_order) as total_purchase_amount,
        |  (SELECT count(distinct supplier_id) FROM fact_purchase_order) as total_suppliers,
        |  -- 产品指标
        |  (SELECT count(distinct product_id) FROM dim_product) as total_products,
        |  -- 同比/环比增长率 (这里需要访问历史数据，示例使用固定值)
        |  0.15 as sales_yoy_growth,
        |  0.05 as sales_mom_growth,
        |  -- 库存指标 (使用近似计算)
        |  (SELECT 
        |     sum(pi.quantity) - sum(si.quantity) 
        |   FROM fact_purchase_order_item pi 
        |   JOIN fact_sales_order_item si ON pi.product_id = si.product_id) as total_inventory
        |""".stripMargin)
  }
  
  /**
   * 保存分析结果到Doris
   */
  def saveToDoris(df: DataFrame, tableName: String): Unit = {
    if (df == null || df.isEmpty) {
      logger.warn(s"待保存的数据集 $tableName 为空，跳过保存")
      return
    }
    
    logger.info(s"将分析结果保存到Doris表: $tableName")
    
    try {
      val dorisOptions = Map(
        "doris.table.identifier" -> s"${ConfigUtils.getDorisDb()}.$tableName",
        "doris.fenodes" -> ConfigUtils.getDorisUrl(),
        "user" -> ConfigUtils.getDorisUser(),
        "password" -> ConfigUtils.getDorisPassword()
      )
      
      df.write.format("doris")
        .options(dorisOptions)
        .option("doris.sink.properties.format", "json")
        .option("doris.sink.batch.size", "10000")
        .option("doris.sink.max.retries", "3")
        .mode("append")
        .save()
      
      logger.info(s"成功保存分析结果到Doris表: $tableName, 记录条数: ${df.count()}")
    } catch {
      case e: Exception =>
        logger.error(s"保存数据到Doris表 $tableName 失败: ${e.getMessage}", e)
        throw e
    }
  }
} 