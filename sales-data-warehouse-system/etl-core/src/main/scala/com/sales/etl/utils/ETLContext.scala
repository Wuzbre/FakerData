package com.sales.etl.utils

import org.apache.spark.sql.SparkSession
import java.time.{LocalDateTime, Duration}
import scala.util.{Try, Success, Failure}

/**
 * Context for ETL process execution
 */
class ETLContext private (
  val jobId: String,
  val startTime: LocalDateTime,
  private var _status: ETLStatus = ETLStatus.Running
) {
  private val metrics = MetricsCollector.getInstance
  private var endTime: Option[LocalDateTime] = None
  private var error: Option[Throwable] = None
  private var currentPhase: String = "Initialization"
  private val phaseTimings = scala.collection.mutable.Map[String, Duration]()

  /**
   * Get current status
   * @return Current ETL status
   */
  def status: ETLStatus = _status

  /**
   * Get execution duration
   * @return Duration of execution
   */
  def duration: Duration = {
    val end = endTime.getOrElse(LocalDateTime.now())
    Duration.between(startTime, end)
  }

  /**
   * Get current phase
   * @return Current execution phase
   */
  def getCurrentPhase: String = currentPhase

  /**
   * Get phase timings
   * @return Map of phase names to durations
   */
  def getPhaseTimings: Map[String, Duration] = phaseTimings.toMap

  /**
   * Start a new phase
   * @param phaseName Name of the phase
   */
  def startPhase(phaseName: String): Unit = {
    metrics.startTimer(s"phase_$phaseName")
    currentPhase = phaseName
    LogUtils.logPhaseStart(phaseName)
  }

  /**
   * End current phase
   */
  def endPhase(): Unit = {
    val duration = metrics.stopTimer(s"phase_$currentPhase")
    phaseTimings(currentPhase) = Duration.ofMillis(duration)
    LogUtils.logPhaseEnd(currentPhase)
  }

  /**
   * Mark ETL as completed
   */
  def markCompleted(): Unit = {
    _status = ETLStatus.Completed
    endTime = Some(LocalDateTime.now())
    logFinalStatus()
  }

  /**
   * Mark ETL as failed
   * @param throwable Error that caused the failure
   */
  def markFailed(throwable: Throwable): Unit = {
    _status = ETLStatus.Failed
    endTime = Some(LocalDateTime.now())
    error = Some(throwable)
    logFinalStatus()
  }

  /**
   * Log final ETL status
   */
  private def logFinalStatus(): Unit = {
    val statusDetails = Map(
      "jobId" -> jobId,
      "status" -> status.toString,
      "startTime" -> startTime.toString,
      "endTime" -> endTime.map(_.toString).getOrElse("N/A"),
      "duration" -> duration.toString,
      "error" -> error.map(_.getMessage).getOrElse("None")
    )

    LogUtils.logOperation(
      "ETL Execution",
      if (status == ETLStatus.Completed) "success" else "failure",
      Some(s"""
        |Final Status:
        |${statusDetails.map { case (k, v) => s"$k: $v" }.mkString("\n")}
        |
        |Phase Timings:
        |${phaseTimings.map { case (phase, time) => s"$phase: $time" }.mkString("\n")}
        """.stripMargin)
    )

    // Record final metrics
    metrics.setMetric("etl_status", status.toString)
    metrics.setMetric("etl_duration", duration.toMillis)
    metrics.setMetric("etl_phases", phaseTimings.size)
    phaseTimings.foreach { case (phase, time) =>
      metrics.setMetric(s"phase_duration_$phase", time.toMillis)
    }
  }
}

/**
 * Companion object for ETLContext
 */
object ETLContext {
  private var currentContext: Option[ETLContext] = None

  /**
   * Initialize new ETL context
   * @param jobId Unique job identifier
   * @return New ETLContext instance
   */
  def initialize(jobId: String): ETLContext = {
    currentContext.foreach { ctx =>
      if (ctx.status == ETLStatus.Running) {
        throw new IllegalStateException("Another ETL context is already running")
      }
    }

    val context = new ETLContext(jobId, LocalDateTime.now())
    currentContext = Some(context)
    
    // Initialize required components
    Try {
      ConfigurationManager.initialize()
      SparkSessionManager.getOrCreate
      LogUtils.logOperation("ETL Context", "success", Some(s"Initialized ETL context for job $jobId"))
    } match {
      case Success(_) => // Initialization successful
      case Failure(e) =>
        context.markFailed(e)
        throw e
    }

    context
  }

  /**
   * Get current ETL context
   * @return Current context if exists
   */
  def current: Option[ETLContext] = currentContext

  /**
   * Clear current context
   */
  def clear(): Unit = {
    currentContext = None
    SparkSessionManager.stop()
  }
}

/**
 * ETL execution status
 */
sealed trait ETLStatus
object ETLStatus {
  case object Running extends ETLStatus
  case object Completed extends ETLStatus
  case object Failed extends ETLStatus
} 