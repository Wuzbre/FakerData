package com.sales.etl.utils

import org.apache.spark.sql.{DataFrame, Column, functions => F}
import scala.util.{Try, Success, Failure}

/**
 * Utility class for checking data quality
 */
object DataQualityChecker {
  private val metrics = MetricsCollector.getInstance
  private lazy val config = ConfigurationManager.getDataQualityConfig

  /**
   * Run all enabled data quality checks
   * @param df DataFrame to check
   * @param tableName Name of the table being checked
   * @return DataQualityResult containing check results
   */
  def checkDataQuality(df: DataFrame, tableName: String): DataQualityResult = {
    metrics.startTimer(s"dataQuality_$tableName")
    
    val results = DataQualityResult(tableName)
    
    Try {
      if (config.nullCheckEnabled) {
        results.nullCheckResults = checkNullValues(df)
      }
      
      if (config.duplicateCheckEnabled) {
        results.duplicateCheckResults = checkDuplicates(df)
      }
      
      if (config.integrityCheckEnabled) {
        results.integrityCheckResults = checkDataIntegrity(df)
      }
      
      results.executionTime = metrics.stopTimer(s"dataQuality_$tableName")
      logResults(results)
      
      results
    } match {
      case Success(r) => r
      case Failure(e) =>
        LogUtils.logOperation(
          "Data Quality Check",
          "failure",
          Some(s"Failed to check data quality for $tableName: ${e.getMessage}")
        )
        throw e
    }
  }

  /**
   * Check for null values in all columns
   * @param df DataFrame to check
   * @return Map of column names to null percentages
   */
  private def checkNullValues(df: DataFrame): Map[String, Double] = {
    val totalCount = df.count().toDouble
    
    df.columns.map { column =>
      val nullCount = df.filter(F.col(column).isNull).count().toDouble
      val nullPercentage = (nullCount / totalCount) * 100
      
      metrics.setMetric(s"nullPercentage_$column", nullPercentage)
      
      column -> nullPercentage
    }.toMap
  }

  /**
   * Check for duplicate records
   * @param df DataFrame to check
   * @return Duplicate check results
   */
  private def checkDuplicates(df: DataFrame): DuplicateCheckResult = {
    val totalCount = df.count()
    val distinctCount = df.distinct().count()
    val duplicateCount = totalCount - distinctCount
    val duplicatePercentage = (duplicateCount.toDouble / totalCount) * 100
    
    metrics.setMetric("duplicateCount", duplicateCount)
    metrics.setMetric("duplicatePercentage", duplicatePercentage)
    
    DuplicateCheckResult(
      totalCount = totalCount,
      distinctCount = distinctCount,
      duplicateCount = duplicateCount,
      duplicatePercentage = duplicatePercentage
    )
  }

  /**
   * Check data integrity rules
   * @param df DataFrame to check
   * @return Map of rule names to check results
   */
  private def checkDataIntegrity(df: DataFrame): Map[String, Boolean] = {
    val checks = Map(
      "hasValidDates" -> checkDateValues(df),
      "hasValidNumbers" -> checkNumericValues(df),
      "hasValidStrings" -> checkStringValues(df)
    )
    
    checks.foreach { case (checkName, result) =>
      metrics.setMetric(s"integrityCheck_$checkName", result)
    }
    
    checks
  }

  /**
   * Check date values in date/timestamp columns
   * @param df DataFrame to check
   * @return true if all date values are valid
   */
  private def checkDateValues(df: DataFrame): Boolean = {
    val dateColumns = df.schema.fields.filter(f => 
      f.dataType.typeName.toLowerCase.contains("date") || 
      f.dataType.typeName.toLowerCase.contains("timestamp")
    ).map(_.name)
    
    dateColumns.forall { column =>
      val invalidDates = df.filter(F.col(column).isNotNull && 
        (F.col(column) < F.lit("1900-01-01") || F.col(column) > F.lit("2100-12-31")))
        .count()
      invalidDates == 0
    }
  }

  /**
   * Check numeric values in numeric columns
   * @param df DataFrame to check
   * @return true if all numeric values are valid
   */
  private def checkNumericValues(df: DataFrame): Boolean = {
    val numericColumns = df.schema.fields.filter(f => 
      f.dataType.typeName.toLowerCase.contains("int") || 
      f.dataType.typeName.toLowerCase.contains("double") ||
      f.dataType.typeName.toLowerCase.contains("decimal")
    ).map(_.name)
    
    numericColumns.forall { column =>
      val invalidNumbers = df.filter(F.col(column).isNotNull && 
        (F.col(column) < F.lit(Double.MinValue) || F.col(column) > F.lit(Double.MaxValue)))
        .count()
      invalidNumbers == 0
    }
  }

  /**
   * Check string values in string columns
   * @param df DataFrame to check
   * @return true if all string values are valid
   */
  private def checkStringValues(df: DataFrame): Boolean = {
    val stringColumns = df.schema.fields.filter(_.dataType.typeName.toLowerCase.contains("string"))
      .map(_.name)
    
    stringColumns.forall { column =>
      val emptyStrings = df.filter(F.col(column).isNotNull && F.trim(F.col(column)) === "")
        .count()
      emptyStrings == 0
    }
  }

  /**
   * Log data quality check results
   * @param results DataQualityResult to log
   */
  private def logResults(results: DataQualityResult): Unit = {
    LogUtils.logOperation(
      "Data Quality Check",
      "success",
      Some(s"""
        |Table: ${results.tableName}
        |Execution Time: ${results.executionTime}ms
        |Null Check Results: ${results.nullCheckResults.map { case (col, pct) => s"$col: $pct%" }.mkString(", ")}
        |Duplicate Check Results: ${results.duplicateCheckResults.duplicatePercentage}% duplicates
        |Integrity Check Results: ${results.integrityCheckResults.map { case (rule, passed) => s"$rule: $passed" }.mkString(", ")}
        """.stripMargin)
    )
  }
}

/**
 * Case class for duplicate check results
 */
case class DuplicateCheckResult(
  totalCount: Long,
  distinctCount: Long,
  duplicateCount: Long,
  duplicatePercentage: Double
)

/**
 * Case class for overall data quality check results
 */
case class DataQualityResult(
  tableName: String,
  var nullCheckResults: Map[String, Double] = Map.empty,
  var duplicateCheckResults: DuplicateCheckResult = DuplicateCheckResult(0, 0, 0, 0.0),
  var integrityCheckResults: Map[String, Boolean] = Map.empty,
  var executionTime: Long = 0
) {
  def hasIssues: Boolean = {
    val config = ConfigurationManager.getDataQualityConfig
    
    val nullIssues = nullCheckResults.exists { case (_, percentage) =>
      percentage > config.thresholds("nullThreshold")
    }
    
    val duplicateIssues = 
      duplicateCheckResults.duplicatePercentage > config.thresholds("duplicateThreshold")
    
    val integrityIssues = integrityCheckResults.exists { case (_, passed) => !passed }
    
    nullIssues || duplicateIssues || integrityIssues
  }
} 