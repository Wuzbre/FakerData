package com.sales.etl.utils

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.logging.log4j.core.config.Configurator
import scala.util.{Try, Success, Failure}

/**
 * Utility object for logging operations in the ETL process
 */
object LogUtils {
  private val logger: Logger = LogManager.getLogger(getClass)

  /**
   * Initialize logging system with custom configuration
   * @param configPath Optional path to log4j2.xml configuration file
   */
  def initializeLogging(configPath: Option[String] = None): Unit = {
    try {
      configPath match {
        case Some(path) => Configurator.initialize(null, path)
        case None => // Use default configuration
      }
      logger.info("Logging system initialized successfully")
    } catch {
      case e: Exception =>
        System.err.println(s"Failed to initialize logging system: ${e.getMessage}")
        throw e
    }
  }

  /**
   * Log execution time of a block of code
   * @param blockName Name of the code block to measure
   * @param block Code block to execute and measure
   * @return Result of the code block
   */
  def logExecutionTime[T](blockName: String)(block: => T): T = {
    val startTime = System.currentTimeMillis()
    try {
      val result = block
      val endTime = System.currentTimeMillis()
      logger.info(s"$blockName completed in ${endTime - startTime}ms")
      result
    } catch {
      case e: Exception =>
        val endTime = System.currentTimeMillis()
        logger.error(s"$blockName failed after ${endTime - startTime}ms", e)
        throw e
    }
  }

  /**
   * Log the start of an ETL phase
   * @param phaseName Name of the ETL phase
   */
  def logPhaseStart(phaseName: String): Unit = {
    logger.info(s"=== Starting ETL phase: $phaseName ===")
  }

  /**
   * Log the completion of an ETL phase
   * @param phaseName Name of the ETL phase
   */
  def logPhaseEnd(phaseName: String): Unit = {
    logger.info(s"=== Completed ETL phase: $phaseName ===")
  }

  /**
   * Log an operation with its status and optional details
   * @param operation Name of the operation
   * @param status Status of the operation (success/failure)
   * @param details Optional additional details
   */
  def logOperation(operation: String, status: String, details: Option[String] = None): Unit = {
    val message = details match {
      case Some(d) => s"Operation: $operation | Status: $status | Details: $d"
      case None => s"Operation: $operation | Status: $status"
    }
    if (status.toLowerCase == "success") {
      logger.info(message)
    } else {
      logger.error(message)
    }
  }

  /**
   * Log metrics about the ETL process
   * @param metrics Map of metric names to values
   */
  def logMetrics(metrics: Map[String, Any]): Unit = {
    logger.info("=== ETL Metrics ===")
    metrics.foreach { case (name, value) =>
      logger.info(s"$name: $value")
    }
  }

  /**
   * Safely execute a block of code and log any errors
   * @param operation Name of the operation
   * @param block Code block to execute
   * @return Try containing the result or failure
   */
  def tryWithLogging[T](operation: String)(block: => T): Try[T] = {
    Try(block) match {
      case Success(result) =>
        logOperation(operation, "success")
        Success(result)
      case Failure(e) =>
        logOperation(operation, "failure", Some(e.getMessage))
        Failure(e)
    }
  }
} 