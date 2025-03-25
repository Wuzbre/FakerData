package com.sales.etl.utils

import com.typesafe.config.{Config, ConfigFactory}
import scala.util.{Try, Success, Failure}
import scala.collection.JavaConverters._

/**
 * Configuration manager for ETL application
 */
object ConfigurationManager {
  private var config: Config = _

  /**
   * Initialize configuration from file
   * @param configPath Optional path to configuration file
   */
  def initialize(configPath: Option[String] = None): Unit = {
    config = configPath match {
      case Some(path) => ConfigFactory.load(path)
      case None => ConfigFactory.load()
    }
  }

  /**
   * Get Spark configuration
   * @return Map of Spark configuration properties
   */
  def getSparkConfig: Map[String, String] = {
    Try {
      config.getConfig("spark").entrySet().asScala.map { entry =>
        (entry.getKey, entry.getValue.unwrapped().toString)
      }.toMap
    }.getOrElse(Map.empty[String, String])
  }

  /**
   * Get MySQL configuration
   * @return MySQL configuration properties
   */
  def getMySQLConfig: MySQLConfig = {
    val mysqlConfig = config.getConfig("mysql")
    MySQLConfig(
      host = mysqlConfig.getString("host"),
      port = mysqlConfig.getInt("port"),
      database = mysqlConfig.getString("database"),
      user = mysqlConfig.getString("user"),
      password = mysqlConfig.getString("password"),
      fetchSize = mysqlConfig.getInt("fetchSize")
    )
  }

  /**
   * Get Hive configuration
   * @return Hive configuration properties
   */
  def getHiveConfig: HiveConfig = {
    val hiveConfig = config.getConfig("hive")
    HiveConfig(
      warehouse = hiveConfig.getString("warehouse"),
      database = hiveConfig.getString("database"),
      tempView = hiveConfig.getString("tempView")
    )
  }

  /**
   * Get data quality configuration
   * @return Data quality configuration properties
   */
  def getDataQualityConfig: DataQualityConfig = {
    val dqConfig = config.getConfig("dataQuality")
    DataQualityConfig(
      nullCheckEnabled = dqConfig.getBoolean("nullCheckEnabled"),
      duplicateCheckEnabled = dqConfig.getBoolean("duplicateCheckEnabled"),
      integrityCheckEnabled = dqConfig.getBoolean("integrityCheckEnabled"),
      thresholds = Map(
        "nullThreshold" -> dqConfig.getDouble("thresholds.nullThreshold"),
        "duplicateThreshold" -> dqConfig.getDouble("thresholds.duplicateThreshold")
      )
    )
  }

  /**
   * Get metrics configuration
   * @return Metrics configuration properties
   */
  def getMetricsConfig: MetricsConfig = {
    val metricsConfig = config.getConfig("metrics")
    MetricsConfig(
      enabled = metricsConfig.getBoolean("enabled"),
      reportInterval = metricsConfig.getDuration("reportInterval"),
      customMetrics = Try {
        metricsConfig.getStringList("customMetrics").asScala.toList
      }.getOrElse(List.empty)
    )
  }

  /**
   * Get dimension table configuration
   * @param dimensionName Name of the dimension
   * @return Dimension configuration properties
   */
  def getDimensionConfig(dimensionName: String): DimensionConfig = {
    val dimConfig = config.getConfig(s"dimensions.$dimensionName")
    DimensionConfig(
      tableName = dimConfig.getString("tableName"),
      sourceQuery = dimConfig.getString("sourceQuery"),
      keyColumns = dimConfig.getStringList("keyColumns").asScala.toList,
      scdType = dimConfig.getInt("scdType"),
      updateColumns = dimConfig.getStringList("updateColumns").asScala.toList
    )
  }

  /**
   * Get fact table configuration
   * @param factName Name of the fact table
   * @return Fact configuration properties
   */
  def getFactConfig(factName: String): FactConfig = {
    val factConfig = config.getConfig(s"facts.$factName")
    FactConfig(
      tableName = factConfig.getString("tableName"),
      sourceQuery = factConfig.getString("sourceQuery"),
      dimensions = factConfig.getStringList("dimensions").asScala.toList,
      measures = factConfig.getStringList("measures").asScala.toList,
      partitionColumns = Try {
        factConfig.getStringList("partitionColumns").asScala.toList
      }.getOrElse(List.empty)
    )
  }

  /**
   * Get string value from configuration
   * @param path Configuration path
   * @param default Default value if path doesn't exist
   * @return Configuration value
   */
  def getString(path: String, default: String = ""): String = {
    Try(config.getString(path)).getOrElse(default)
  }

  /**
   * Get integer value from configuration
   * @param path Configuration path
   * @param default Default value if path doesn't exist
   * @return Configuration value
   */
  def getInt(path: String, default: Int = 0): Int = {
    Try(config.getInt(path)).getOrElse(default)
  }

  /**
   * Get boolean value from configuration
   * @param path Configuration path
   * @param default Default value if path doesn't exist
   * @return Configuration value
   */
  def getBoolean(path: String, default: Boolean = false): Boolean = {
    Try(config.getBoolean(path)).getOrElse(default)
  }
}

// Configuration case classes
case class MySQLConfig(
  host: String,
  port: Int,
  database: String,
  user: String,
  password: String,
  fetchSize: Int
)

case class HiveConfig(
  warehouse: String,
  database: String,
  tempView: String
)

case class DataQualityConfig(
  nullCheckEnabled: Boolean,
  duplicateCheckEnabled: Boolean,
  integrityCheckEnabled: Boolean,
  thresholds: Map[String, Double]
)

case class MetricsConfig(
  enabled: Boolean,
  reportInterval: java.time.Duration,
  customMetrics: List[String]
)

case class DimensionConfig(
  tableName: String,
  sourceQuery: String,
  keyColumns: List[String],
  scdType: Int,
  updateColumns: List[String]
)

case class FactConfig(
  tableName: String,
  sourceQuery: String,
  dimensions: List[String],
  measures: List[String],
  partitionColumns: List[String]
) 