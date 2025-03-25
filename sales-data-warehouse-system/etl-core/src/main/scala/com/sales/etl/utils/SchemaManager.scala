package com.sales.etl.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import scala.util.{Try, Success, Failure}

/**
 * Manages table schemas and their evolution
 */
object SchemaManager {
  private val metrics = MetricsCollector.getInstance

  /**
   * Validate and evolve schema if needed
   * @param df DataFrame to validate
   * @param tableName Table name
   * @param expectedSchema Expected schema
   * @return Updated DataFrame with correct schema
   */
  def validateAndEvolveSchema(
    df: DataFrame,
    tableName: String,
    expectedSchema: StructType
  ): DataFrame = {
    metrics.startTimer(s"schemaValidation_$tableName")
    
    Try {
      val currentSchema = df.schema
      val differences = findSchemaDifferences(currentSchema, expectedSchema)
      
      if (differences.isEmpty) {
        LogUtils.logOperation(
          "Schema Validation",
          "success",
          Some(s"Schema for $tableName matches expected schema")
        )
        df
      } else {
        LogUtils.logOperation(
          "Schema Evolution",
          "info",
          Some(s"Evolving schema for $tableName: ${differences.mkString(", ")}")
        )
        evolveSchema(df, differences)
      }
    } match {
      case Success(result) =>
        metrics.stopTimer(s"schemaValidation_$tableName")
        result
      case Failure(e) =>
        LogUtils.logOperation(
          "Schema Validation",
          "failure",
          Some(s"Failed to validate/evolve schema for $tableName: ${e.getMessage}")
        )
        throw e
    }
  }

  /**
   * Find differences between current and expected schema
   * @param current Current schema
   * @param expected Expected schema
   * @return List of schema differences
   */
  private def findSchemaDifferences(
    current: StructType,
    expected: StructType
  ): List[SchemaDifference] = {
    val differences = scala.collection.mutable.ListBuffer[SchemaDifference]()
    
    // Check for missing columns
    expected.fields.foreach { expectedField =>
      current.fields.find(_.name == expectedField.name) match {
        case None =>
          differences += MissingColumn(expectedField.name, expectedField.dataType)
        case Some(currentField) if currentField.dataType != expectedField.dataType =>
          differences += TypeMismatch(
            expectedField.name,
            currentField.dataType,
            expectedField.dataType
          )
      }
    }
    
    // Check for extra columns
    current.fields.foreach { currentField =>
      if (!expected.fields.exists(_.name == currentField.name)) {
        differences += ExtraColumn(currentField.name, currentField.dataType)
      }
    }
    
    differences.toList
  }

  /**
   * Evolve schema based on differences
   * @param df DataFrame to evolve
   * @param differences Schema differences to address
   * @return Updated DataFrame
   */
  private def evolveSchema(
    df: DataFrame,
    differences: List[SchemaDifference]
  ): DataFrame = {
    var evolvedDf = df
    
    differences.foreach {
      case MissingColumn(name, dataType) =>
        evolvedDf = addMissingColumn(evolvedDf, name, dataType)
      
      case TypeMismatch(name, currentType, expectedType) =>
        evolvedDf = castColumn(evolvedDf, name, expectedType)
      
      case ExtraColumn(name, _) =>
        evolvedDf = dropExtraColumn(evolvedDf, name)
    }
    
    evolvedDf
  }

  /**
   * Add missing column with default value
   * @param df DataFrame to modify
   * @param columnName Column name
   * @param dataType Column data type
   * @return Updated DataFrame
   */
  private def addMissingColumn(
    df: DataFrame,
    columnName: String,
    dataType: DataType
  ): DataFrame = {
    val defaultValue = getDefaultValue(dataType)
    df.withColumn(columnName, org.apache.spark.sql.functions.lit(defaultValue))
  }

  /**
   * Cast column to expected type
   * @param df DataFrame to modify
   * @param columnName Column name
   * @param expectedType Expected data type
   * @return Updated DataFrame
   */
  private def castColumn(
    df: DataFrame,
    columnName: String,
    expectedType: DataType
  ): DataFrame = {
    df.withColumn(columnName, df(columnName).cast(expectedType))
  }

  /**
   * Drop extra column
   * @param df DataFrame to modify
   * @param columnName Column to drop
   * @return Updated DataFrame
   */
  private def dropExtraColumn(
    df: DataFrame,
    columnName: String
  ): DataFrame = {
    df.drop(columnName)
  }

  /**
   * Get default value for data type
   * @param dataType Data type
   * @return Default value
   */
  private def getDefaultValue(dataType: DataType): Any = {
    dataType match {
      case _: StringType => null
      case _: IntegerType => null
      case _: LongType => null
      case _: DoubleType => null
      case _: FloatType => null
      case _: BooleanType => null
      case _: TimestampType => null
      case _: DateType => null
      case _: DecimalType => null
      case _ => null
    }
  }

  /**
   * Create table with specified schema
   * @param spark SparkSession
   * @param tableName Table name
   * @param schema Table schema
   * @param location Table location
   * @param format Table format
   */
  def createTable(
    spark: SparkSession,
    tableName: String,
    schema: StructType,
    location: String,
    format: String = "parquet"
  ): Unit = {
    metrics.startTimer(s"tableCreation_$tableName")
    
    Try {
      val createTableSQL = s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          ${schema.fields.map(f => s"${f.name} ${f.dataType.sql}").mkString(",\n  ")}
        )
        USING $format
        LOCATION '$location'
      """
      
      spark.sql(createTableSQL)
      
      LogUtils.logOperation(
        "Table Creation",
        "success",
        Some(s"Created table $tableName at $location")
      )
    } match {
      case Success(_) =>
        metrics.stopTimer(s"tableCreation_$tableName")
      case Failure(e) =>
        LogUtils.logOperation(
          "Table Creation",
          "failure",
          Some(s"Failed to create table $tableName: ${e.getMessage}")
        )
        throw e
    }
  }
}

/**
 * Schema difference types
 */
sealed trait SchemaDifference
case class MissingColumn(name: String, dataType: DataType) extends SchemaDifference
case class TypeMismatch(name: String, currentType: DataType, expectedType: DataType) extends SchemaDifference
case class ExtraColumn(name: String, dataType: DataType) extends SchemaDifference 