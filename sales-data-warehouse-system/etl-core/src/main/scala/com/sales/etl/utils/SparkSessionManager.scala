package com.sales.etl.utils

import org.apache.spark.sql.SparkSession
import scala.util.{Try, Success, Failure}

/**
 * Manages Spark session creation and configuration
 */
object SparkSessionManager {
  private var sparkSession: SparkSession = _

  /**
   * Initialize or get existing Spark session
   * @return SparkSession instance
   */
  def getOrCreate: SparkSession = {
    if (sparkSession == null || sparkSession.sparkContext.isStopped) {
      initialize()
    }
    sparkSession
  }

  /**
   * Initialize Spark session with configurations
   */
  private def initialize(): Unit = {
    try {
      val sparkConfig = ConfigurationManager.getSparkConfig
      val builder = SparkSession.builder()
        .appName(sparkConfig.getOrElse("spark.appName", "Sales Data Warehouse ETL"))
        .enableHiveSupport()

      // Apply master configuration if provided
      sparkConfig.get("spark.master").foreach(builder.master)

      // Build initial session
      sparkSession = builder.getOrCreate()

      // Apply additional configurations
      sparkConfig.foreach { case (key, value) =>
        if (!key.startsWith("spark.master") && !key.startsWith("spark.app")) {
          sparkSession.conf.set(key, value)
        }
      }

      // Set up logging
      sparkSession.sparkContext.setLogLevel(
        sparkConfig.getOrElse("spark.log.level", "WARN")
      )

      LogUtils.logOperation(
        "SparkSession Initialization",
        "success",
        Some(s"Created Spark session with app name: ${sparkSession.conf.get("spark.app.name")}")
      )
    } catch {
      case e: Exception =>
        LogUtils.logOperation(
          "SparkSession Initialization",
          "failure",
          Some(s"Failed to create Spark session: ${e.getMessage}")
        )
        throw e
    }
  }

  /**
   * Stop Spark session if active
   */
  def stop(): Unit = {
    Option(sparkSession).foreach { session =>
      if (!session.sparkContext.isStopped) {
        Try {
          session.stop()
          LogUtils.logOperation("SparkSession Stop", "success")
        } match {
          case Failure(e) =>
            LogUtils.logOperation(
              "SparkSession Stop",
              "failure",
              Some(s"Failed to stop Spark session: ${e.getMessage}")
            )
          case Success(_) => // Already logged success
        }
      }
    }
    sparkSession = null
  }

  /**
   * Get current Spark session configuration
   * @return Map of configuration key-value pairs
   */
  def getCurrentConfig: Map[String, String] = {
    if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
      sparkSession.conf.getAll
    } else {
      Map.empty[String, String]
    }
  }

  /**
   * Update Spark session configuration
   * @param configs Map of configuration key-value pairs to update
   */
  def updateConfig(configs: Map[String, String]): Unit = {
    if (sparkSession != null && !sparkSession.sparkContext.isStopped) {
      configs.foreach { case (key, value) =>
        Try {
          sparkSession.conf.set(key, value)
          LogUtils.logOperation(
            "Spark Configuration Update",
            "success",
            Some(s"Updated config: $key = $value")
          )
        } match {
          case Failure(e) =>
            LogUtils.logOperation(
              "Spark Configuration Update",
              "failure",
              Some(s"Failed to update config $key: ${e.getMessage}")
            )
          case Success(_) => // Already logged success
        }
      }
    } else {
      throw new IllegalStateException("Spark session is not initialized")
    }
  }

  /**
   * Check if Spark session is active
   * @return true if session is active, false otherwise
   */
  def isActive: Boolean = {
    sparkSession != null && !sparkSession.sparkContext.isStopped
  }

  /**
   * Get Spark session metrics
   * @return Map of metrics
   */
  def getMetrics: Map[String, Long] = {
    if (isActive) {
      val sc = sparkSession.sparkContext
      Map(
        "activeJobs" -> sc.statusTracker.getActiveJobIds().length,
        "allJobs" -> sc.statusTracker.getJobIdsForGroup().length,
        "activeStages" -> sc.statusTracker.getActiveStageIds().length,
        "allStages" -> sc.statusTracker.getStageIdsForGroup().length
      )
    } else {
      Map.empty[String, Long]
    }
  }
} 