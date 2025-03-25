package com.sales.etl.utils

import scala.collection.mutable
import java.time.{Instant, Duration}

/**
 * Utility class for collecting and managing ETL metrics
 */
class MetricsCollector {
  private val metrics = mutable.Map[String, Any]()
  private val timers = mutable.Map[String, Instant]()
  private val counters = mutable.Map[String, Long]()

  /**
   * Start timing an operation
   * @param operationName Name of the operation to time
   */
  def startTimer(operationName: String): Unit = {
    timers(operationName) = Instant.now()
  }

  /**
   * Stop timing an operation and record the duration
   * @param operationName Name of the operation
   * @return Duration of the operation in milliseconds
   */
  def stopTimer(operationName: String): Long = {
    timers.get(operationName) match {
      case Some(startTime) =>
        val duration = Duration.between(startTime, Instant.now()).toMillis
        metrics(s"${operationName}Duration") = duration
        timers.remove(operationName)
        duration
      case None =>
        throw new IllegalStateException(s"Timer for operation $operationName was not started")
    }
  }

  /**
   * Increment a counter
   * @param counterName Name of the counter
   * @param value Value to increment by (default 1)
   */
  def incrementCounter(counterName: String, value: Long = 1): Unit = {
    counters(counterName) = counters.getOrElse(counterName, 0L) + value
    metrics(counterName) = counters(counterName)
  }

  /**
   * Set a metric value
   * @param name Metric name
   * @param value Metric value
   */
  def setMetric(name: String, value: Any): Unit = {
    metrics(name) = value
  }

  /**
   * Get a specific metric value
   * @param name Metric name
   * @return Option containing the metric value if it exists
   */
  def getMetric(name: String): Option[Any] = {
    metrics.get(name)
  }

  /**
   * Get a counter value
   * @param name Counter name
   * @return Current counter value or 0 if not found
   */
  def getCounter(name: String): Long = {
    counters.getOrElse(name, 0L)
  }

  /**
   * Get all metrics as an immutable map
   * @return Map of all collected metrics
   */
  def getAllMetrics: Map[String, Any] = {
    metrics.toMap
  }

  /**
   * Reset all metrics, timers, and counters
   */
  def reset(): Unit = {
    metrics.clear()
    timers.clear()
    counters.clear()
  }

  /**
   * Record success/failure metrics for an operation
   * @param operationName Name of the operation
   * @param success Whether the operation succeeded
   */
  def recordOperationResult(operationName: String, success: Boolean): Unit = {
    val successKey = s"${operationName}Success"
    val failureKey = s"${operationName}Failure"
    if (success) {
      incrementCounter(successKey)
    } else {
      incrementCounter(failureKey)
    }
  }

  /**
   * Record batch processing metrics
   * @param batchName Name of the batch
   * @param recordsProcessed Number of records processed
   * @param successCount Number of successful records
   * @param failureCount Number of failed records
   */
  def recordBatchMetrics(batchName: String, recordsProcessed: Long, 
                        successCount: Long, failureCount: Long): Unit = {
    setMetric(s"${batchName}RecordsProcessed", recordsProcessed)
    setMetric(s"${batchName}SuccessCount", successCount)
    setMetric(s"${batchName}FailureCount", failureCount)
    if (recordsProcessed > 0) {
      val successRate = (successCount.toDouble / recordsProcessed) * 100
      setMetric(s"${batchName}SuccessRate", f"$successRate%.2f%%")
    }
  }

  /**
   * Log all collected metrics using LogUtils
   */
  def logMetrics(): Unit = {
    LogUtils.logMetrics(getAllMetrics)
  }
}

/**
 * Companion object for MetricsCollector
 */
object MetricsCollector {
  private val instance = new MetricsCollector()
  
  def getInstance: MetricsCollector = instance
} 