package com.sales

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import scopt.OParser
import com.sales.etl.{MysqlToOds, OdsToDwd, DwdToDws}
import com.sales.quality.DataQualityCheck
import com.sales.sync.HiveToDoris
import com.sales.utils.{ConfigUtils, DateUtils}
import java.io.{File, PrintWriter}
import scala.util.{Try, Success, Failure}

/**
 * 销售数据ETL主入口类
 * 负责解析命令行参数，创建Spark会话，执行ETL流程
 */
object SalesDataETL {
  private val logger = Logger.getLogger(this.getClass)
  
  // 命令行参数配置
  case class Config(
    date: String = DateUtils.getYesterdayCompact(),
    mysqlToOds: Boolean = false,
    odsToDwd: Boolean = false,
    dwdToDws: Boolean = false,
    dqCheck: Boolean = false,
    syncToDoris: Boolean = false,
    all: Boolean = false,
    cleanExpired: Boolean = false,
    retentionDays: Int = ConfigUtils.PARTITION_RETENTION_DAYS
  )
  
  def main(args: Array[String]): Unit = {
    val builder = OParser.builder[Config]
    val parser = {
      import builder._
      OParser.sequence(
        programName("sales-data-etl"),
        head("销售数据ETL", "1.0"),
        opt[String]('d', "date")
          .action((x, c) => c.copy(date = x))
          .text("处理日期，格式：yyyyMMdd，默认为昨天"),
        opt[Unit]("mysql-to-ods")
          .action((_, c) => c.copy(mysqlToOds = true))
          .text("执行MySQL到ODS的数据抽取"),
        opt[Unit]("ods-to-dwd")
          .action((_, c) => c.copy(odsToDwd = true))
          .text("执行ODS到DWD的数据转换"),
        opt[Unit]("dwd-to-dws")
          .action((_, c) => c.copy(dwdToDws = true))
          .text("执行DWD到DWS的数据汇总"),
        opt[Unit]("dq-check")
          .action((_, c) => c.copy(dqCheck = true))
          .text("执行数据质量检查"),
        opt[Unit]("sync-to-doris")
          .action((_, c) => c.copy(syncToDoris = true))
          .text("执行Hive到Doris的数据同步"),
        opt[Unit]("all")
          .action((_, c) => c.copy(all = true))
          .text("执行所有ETL流程"),
        opt[Unit]("clean-expired")
          .action((_, c) => c.copy(cleanExpired = true))
          .text("清理过期分区数据"),
        opt[Int]("retention-days")
          .action((x, c) => c.copy(retentionDays = x))
          .text(s"数据保留天数，默认为${ConfigUtils.PARTITION_RETENTION_DAYS}天")
      )
    }
    
    OParser.parse(parser, args, Config()) match {
      case Some(config) =>
        run(config)
      case _ =>
        // 解析参数失败，直接退出
        System.exit(1)
    }
  }
  
  /**
   * 执行ETL流程
   * @param config 配置参数
   */
  def run(config: Config): Unit = {
    logger.info(s"开始执行销售数据ETL流程，处理日期: ${config.date}")
    
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName(s"Sales-Data-ETL-${config.date}")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      var mysqlToOdsSuccess = true
      var odsToDwdSuccess = true
      var dwdToDwsSuccess = true
      var dqCheckSuccess = true
      var syncToDorisSuccess = true
      var cleanExpiredSuccess = true
      
      // 清理过期分区
      if (config.cleanExpired || config.all) {
        logger.info(s"开始清理过期分区，保留天数: ${config.retentionDays}")
        cleanExpiredSuccess = cleanExpiredPartitions(spark, config.date, config.retentionDays)
        if (!cleanExpiredSuccess) {
          logger.error("清理过期分区失败")
        }
      }
      
      // MySQL到ODS
      if (config.mysqlToOds || config.all) {
        logger.info("开始执行MySQL到ODS的数据抽取")
        mysqlToOdsSuccess = MysqlToOds.process(spark, config.date)
        if (!mysqlToOdsSuccess) {
          logger.error("MySQL到ODS的数据抽取失败")
          if (config.all) return  // 如果是all模式，前一步失败则终止流程
        }
      }
      
      // ODS到DWD
      if ((config.odsToDwd || config.all) && mysqlToOdsSuccess) {
        logger.info("开始执行ODS到DWD的数据转换")
        odsToDwdSuccess = OdsToDwd.process(spark, config.date)
        if (!odsToDwdSuccess) {
          logger.error("ODS到DWD的数据转换失败")
          if (config.all) return  // 如果是all模式，前一步失败则终止流程
        }
      }
      
      // DWD到DWS
      if ((config.dwdToDws || config.all) && odsToDwdSuccess) {
        logger.info("开始执行DWD到DWS的数据汇总")
        dwdToDwsSuccess = DwdToDws.process(spark, config.date)
        if (!dwdToDwsSuccess) {
          logger.error("DWD到DWS的数据汇总失败")
          if (config.all) return  // 如果是all模式，前一步失败则终止流程
        }
      }
      
      // 数据质量检查
      if ((config.dqCheck || config.all) && dwdToDwsSuccess) {
        logger.info("开始执行数据质量检查")
        dqCheckSuccess = DataQualityCheck.process(spark, config.date)
        if (!dqCheckSuccess) {
          logger.warn("数据质量检查发现问题，请查看检查结果表")
          // 数据质量问题不阻止后续流程，但会记录警告
        }
      }
      
      // Hive到Doris同步
      if ((config.syncToDoris || config.all) && dwdToDwsSuccess) {
        logger.info("开始执行Hive到Doris的数据同步")
        syncToDorisSuccess = HiveToDoris.process(spark, config.date)
        if (!syncToDorisSuccess) {
          logger.error("Hive到Doris的数据同步失败")
        }
      }
      
      // 生成ETL处理报告
      if (config.all || config.dqCheck) {
        generateReport(spark, config.date, mysqlToOdsSuccess, odsToDwdSuccess, dwdToDwsSuccess, dqCheckSuccess, syncToDorisSuccess)
      }
      
      val allSuccess = (config.cleanExpired && cleanExpiredSuccess || !config.cleanExpired) &&
                       (config.mysqlToOds && mysqlToOdsSuccess || !config.mysqlToOds) &&
                       (config.odsToDwd && odsToDwdSuccess || !config.odsToDwd) &&
                       (config.dwdToDws && dwdToDwsSuccess || !config.dwdToDws) &&
                       (config.dqCheck && dqCheckSuccess || !config.dqCheck) &&
                       (config.syncToDoris && syncToDorisSuccess || !config.syncToDoris) &&
                       (!config.all || (cleanExpiredSuccess && mysqlToOdsSuccess && odsToDwdSuccess && dwdToDwsSuccess && syncToDorisSuccess))
      
      if (allSuccess) {
        logger.info(s"销售数据ETL流程执行成功，处理日期: ${config.date}")
      } else {
        logger.error(s"销售数据ETL流程执行失败，处理日期: ${config.date}")
        System.exit(1)
      }
      
    } catch {
      case e: Exception =>
        logger.error(s"销售数据ETL流程执行异常: ${e.getMessage}", e)
        System.exit(1)
    } finally {
      spark.stop()
    }
  }
  
  /**
   * 清理过期分区
   * @param spark SparkSession
   * @param processDate 处理日期
   * @param retentionDays 保留天数
   * @return 是否成功
   */
  private def cleanExpiredPartitions(spark: SparkSession, processDate: String, retentionDays: Int): Boolean = {
    try {
      logger.info(s"开始清理过期分区，保留最近 $retentionDays 天数据")
      
      // 获取过期分区列表
      val expiredPartitions = DateUtils.getExpiredPartitions(processDate, retentionDays)
      if (expiredPartitions.isEmpty) {
        logger.info("没有找到过期分区需要清理")
        return true
      }
      
      // 清理ODS层过期分区
      Try {
        spark.sql(s"USE ${ConfigUtils.ODS_DATABASE}")
        
        val odsTables = spark.sql("SHOW TABLES").collect().map(_.getString(0))
        
        odsTables.foreach { table =>
          expiredPartitions.foreach { partition =>
            logger.info(s"清理 ${ConfigUtils.ODS_DATABASE}.$table 表的分区 $partition")
            try {
              spark.sql(s"ALTER TABLE $table DROP IF EXISTS PARTITION ($partition)")
            } catch {
              case e: Exception =>
                logger.warn(s"清理 ${ConfigUtils.ODS_DATABASE}.$table 表的分区 $partition 失败: ${e.getMessage}")
            }
          }
        }
      } match {
        case Failure(e) => 
          logger.error(s"清理ODS层过期分区失败: ${e.getMessage}", e)
          return false
        case _ => // 继续处理
      }
      
      // 清理DWD层过期分区
      Try {
        spark.sql(s"USE ${ConfigUtils.DWD_DATABASE}")
        
        val dwdTables = spark.sql("SHOW TABLES").collect().map(_.getString(0))
        
        dwdTables.foreach { table =>
          expiredPartitions.foreach { partition =>
            logger.info(s"清理 ${ConfigUtils.DWD_DATABASE}.$table 表的分区 $partition")
            try {
              spark.sql(s"ALTER TABLE $table DROP IF EXISTS PARTITION ($partition)")
            } catch {
              case e: Exception =>
                logger.warn(s"清理 ${ConfigUtils.DWD_DATABASE}.$table 表的分区 $partition 失败: ${e.getMessage}")
            }
          }
        }
      } match {
        case Failure(e) => 
          logger.error(s"清理DWD层过期分区失败: ${e.getMessage}", e)
          return false
        case _ => // 继续处理
      }
      
      // 清理DWS层过期分区
      Try {
        spark.sql(s"USE ${ConfigUtils.DWS_DATABASE}")
        
        val dwsTables = spark.sql("SHOW TABLES").collect().map(_.getString(0))
        
        dwsTables.foreach { table =>
          expiredPartitions.foreach { partition =>
            logger.info(s"清理 ${ConfigUtils.DWS_DATABASE}.$table 表的分区 $partition")
            try {
              // DWS层使用process_dt作为分区字段
              val processPartition = partition.replace("dt=", "process_dt=")
              spark.sql(s"ALTER TABLE $table DROP IF EXISTS PARTITION ($processPartition)")
            } catch {
              case e: Exception =>
                logger.warn(s"清理 ${ConfigUtils.DWS_DATABASE}.$table 表的分区 $partition 失败: ${e.getMessage}")
            }
          }
        }
      } match {
        case Failure(e) => 
          logger.error(s"清理DWS层过期分区失败: ${e.getMessage}", e)
          return false
        case _ => // 继续处理
      }
      
      logger.info("过期分区清理完成")
      true
    } catch {
      case e: Exception =>
        logger.error(s"清理过期分区异常: ${e.getMessage}", e)
        false
    }
  }
  
  /**
   * 生成ETL处理报告
   */
  private def generateReport(
    spark: SparkSession, 
    processDate: String,
    mysqlToOdsSuccess: Boolean,
    odsToDwdSuccess: Boolean,
    dwdToDwsSuccess: Boolean,
    dqCheckSuccess: Boolean,
    syncToDorisSuccess: Boolean
  ): Unit = {
    try {
      logger.info("开始生成ETL处理报告")
      
      val dateStr = DateUtils.formatDate(processDate)
      val reportFile = new File(s"${ConfigUtils.LOG_PATH}/etl_report_${processDate}.log")
      reportFile.getParentFile.mkdirs()
      
      val writer = new PrintWriter(reportFile)
      
      writer.println("================================================")
      writer.println(s"销售数据ETL处理报告 - ${dateStr}")
      writer.println("================================================")
      writer.println("")
      
      // 处理状态摘要
      writer.println("一、处理状态:")
      writer.println(s"处理日期: ${dateStr}")
      writer.println(s"MySQL到ODS: ${if (mysqlToOdsSuccess) "成功" else "失败"}")
      writer.println(s"ODS到DWD: ${if (odsToDwdSuccess) "成功" else "失败"}")
      writer.println(s"DWD到DWS: ${if (dwdToDwsSuccess) "成功" else "失败"}")
      writer.println(s"数据质量检查: ${if (dqCheckSuccess) "通过" else "发现问题"}")
      writer.println(s"Hive到Doris: ${if (syncToDorisSuccess) "成功" else "失败"}")
      writer.println("")
      
      // 数据量统计
      writer.println("二、数据量统计:")
      
      Try {
        // ODS层数据量
        spark.sql(s"USE ${ConfigUtils.ODS_DATABASE}")
        
        val odsTables = spark.sql("SHOW TABLES").collect().map(_.getString(0))
        
        writer.println("2.1 ODS层数据量:")
        odsTables.foreach { table =>
          val count = spark.sql(s"SELECT COUNT(1) FROM $table WHERE dt='$dateStr'").first().getLong(0)
          writer.println(s"  - $table: $count 条")
        }
        
        // DWD层数据量
        spark.sql(s"USE ${ConfigUtils.DWD_DATABASE}")
        
        val dwdTables = spark.sql("SHOW TABLES").collect().map(_.getString(0))
        
        writer.println("\n2.2 DWD层数据量:")
        dwdTables.foreach { table =>
          val count = spark.sql(s"SELECT COUNT(1) FROM $table WHERE dt='$dateStr'").first().getLong(0)
          writer.println(s"  - $table: $count 条")
        }
        
        // DWS层数据量
        spark.sql(s"USE ${ConfigUtils.DWS_DATABASE}")
        
        val dwsTables = spark.sql("SHOW TABLES").collect().map(_.getString(0))
        
        writer.println("\n2.3 DWS层数据量:")
        dwsTables.foreach { table =>
          val count = spark.sql(s"SELECT COUNT(1) FROM $table WHERE process_dt='$dateStr'").first().getLong(0)
          writer.println(s"  - $table: $count 条")
        }
      } match {
        case Failure(e) => 
          writer.println(s"数据量统计失败: ${e.getMessage}")
        case _ => // 继续处理
      }
      
      // 数据质量问题
      writer.println("\n三、数据质量问题:")
      
      Try {
        // 创建临时视图，查询数据质量检查结果
        spark.sql(
          s"""
             |CREATE OR REPLACE TEMPORARY VIEW dq_issues AS
             |SELECT check_table, check_type, check_result, severity, check_time
             |FROM ${ConfigUtils.DQ_DATABASE}.dq_check_results
             |WHERE dt='$dateStr'
             |  AND check_result > 0
           """.stripMargin)
        
        // 按严重程度统计问题
        val highIssues = spark.sql("SELECT * FROM dq_issues WHERE severity='high' ORDER BY check_time").collect()
        val mediumIssues = spark.sql("SELECT * FROM dq_issues WHERE severity='medium' ORDER BY check_time").collect()
        val lowIssues = spark.sql("SELECT * FROM dq_issues WHERE severity='low' ORDER BY check_time").collect()
        
        writer.println(s"3.1 高严重性问题 (${highIssues.length}):")
        if (highIssues.isEmpty) {
          writer.println("  - 无")
        } else {
          highIssues.foreach { row =>
            writer.println(s"  - 表: ${row.getString(0)}, 检查类型: ${row.getString(1)}, 异常记录数: ${row.getLong(2)}")
          }
        }
        
        writer.println(s"\n3.2 中严重性问题 (${mediumIssues.length}):")
        if (mediumIssues.isEmpty) {
          writer.println("  - 无")
        } else {
          mediumIssues.foreach { row =>
            writer.println(s"  - 表: ${row.getString(0)}, 检查类型: ${row.getString(1)}, 异常记录数: ${row.getLong(2)}")
          }
        }
        
        writer.println(s"\n3.3 低严重性问题 (${lowIssues.length}):")
        if (lowIssues.isEmpty) {
          writer.println("  - 无")
        } else {
          lowIssues.foreach { row =>
            writer.println(s"  - 表: ${row.getString(0)}, 检查类型: ${row.getString(1)}, 异常记录数: ${row.getLong(2)}")
          }
        }
      } match {
        case Failure(e) => 
          writer.println(s"数据质量问题统计失败: ${e.getMessage}")
        case _ => // 继续处理
      }
      
      // 报告总结
      writer.println("\n四、总结:")
      val allSuccess = mysqlToOdsSuccess && odsToDwdSuccess && dwdToDwsSuccess && syncToDorisSuccess
      if (allSuccess && dqCheckSuccess) {
        writer.println("ETL处理完全成功，数据质量检查通过。")
      } else if (allSuccess && !dqCheckSuccess) {
        writer.println("ETL处理完成，但数据质量检查发现问题，请查看上述详情。")
      } else {
        writer.println("ETL处理存在失败环节，请检查相关日志进行修复。")
      }
      
      writer.println("\n报告生成时间: " + DateUtils.getCurrentDateTime())
      writer.println("================================================")
      
      writer.close()
      
      logger.info(s"ETL处理报告已生成: ${reportFile.getAbsolutePath()}")
    } catch {
      case e: Exception =>
        logger.error(s"生成ETL处理报告异常: ${e.getMessage}", e)
    }
  }
}