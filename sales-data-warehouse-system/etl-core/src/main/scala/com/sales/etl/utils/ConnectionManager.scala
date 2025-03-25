package com.sales.etl.utils

import java.sql.{Connection, DriverManager}
import scala.util.{Try, Success, Failure}
import org.apache.spark.sql.{SparkSession, DataFrame}

/**
 * Manages database connections for ETL processes
 */
object ConnectionManager {
  private val metrics = MetricsCollector.getInstance
  private var mysqlConnection: Option[Connection] = None

  /**
   * Get MySQL connection
   * @return MySQL connection
   */
  def getMySQLConnection: Connection = {
    mysqlConnection.getOrElse {
      val config = ConfigurationManager.getMySQLConfig
      
      metrics.startTimer("mysql_connection")
      Try {
        Class.forName("com.mysql.cj.jdbc.Driver")
        val conn = DriverManager.getConnection(
          s"jdbc:mysql://${config.host}:${config.port}/${config.database}",
          config.user,
          config.password
        )
        
        mysqlConnection = Some(conn)
        LogUtils.logOperation(
          "MySQL Connection",
          "success",
          Some(s"Connected to MySQL at ${config.host}:${config.port}")
        )
        
        conn
      } match {
        case Success(conn) =>
          metrics.stopTimer("mysql_connection")
          conn
        case Failure(e) =>
          LogUtils.logOperation(
            "MySQL Connection",
            "failure",
            Some(s"Failed to connect to MySQL: ${e.getMessage}")
          )
          throw e
      }
    }
  }

  /**
   * Read data from MySQL table using Spark
   * @param spark SparkSession
   * @param tableName Table name
   * @param query Optional query
   * @return DataFrame with results
   */
  def readMySQL(
    spark: SparkSession,
    tableName: String,
    query: Option[String] = None
  ): DataFrame = {
    val config = ConfigurationManager.getMySQLConfig
    
    metrics.startTimer(s"mysql_read_$tableName")
    Try {
      val reader = spark.read
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", s"jdbc:mysql://${config.host}:${config.port}/${config.database}")
        .option("user", config.user)
        .option("password", config.password)
        .option("fetchsize", config.fetchSize)
      
      val df = query match {
        case Some(q) => reader.option("query", q)
        case None => reader.option("dbtable", tableName)
      }.load()
      
      LogUtils.logOperation(
        "MySQL Read",
        "success",
        Some(s"Read ${df.count()} records from $tableName")
      )
      
      df
    } match {
      case Success(df) =>
        metrics.stopTimer(s"mysql_read_$tableName")
        df
      case Failure(e) =>
        LogUtils.logOperation(
          "MySQL Read",
          "failure",
          Some(s"Failed to read from $tableName: ${e.getMessage}")
        )
        throw e
    }
  }

  /**
   * Write DataFrame to MySQL table
   * @param df DataFrame to write
   * @param tableName Target table name
   * @param mode Write mode (append/overwrite)
   */
  def writeMySQL(
    df: DataFrame,
    tableName: String,
    mode: String = "append"
  ): Unit = {
    val config = ConfigurationManager.getMySQLConfig
    
    metrics.startTimer(s"mysql_write_$tableName")
    Try {
      df.write
        .format("jdbc")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("url", s"jdbc:mysql://${config.host}:${config.port}/${config.database}")
        .option("user", config.user)
        .option("password", config.password)
        .option("dbtable", tableName)
        .mode(mode)
        .save()
      
      LogUtils.logOperation(
        "MySQL Write",
        "success",
        Some(s"Wrote ${df.count()} records to $tableName")
      )
    } match {
      case Success(_) =>
        metrics.stopTimer(s"mysql_write_$tableName")
      case Failure(e) =>
        LogUtils.logOperation(
          "MySQL Write",
          "failure",
          Some(s"Failed to write to $tableName: ${e.getMessage}")
        )
        throw e
    }
  }

  /**
   * Execute MySQL query
   * @param query SQL query to execute
   * @return Number of affected rows
   */
  def executeMySQL(query: String): Int = {
    metrics.startTimer("mysql_execute")
    Try {
      val conn = getMySQLConnection
      val stmt = conn.createStatement()
      val result = stmt.executeUpdate(query)
      stmt.close()
      
      LogUtils.logOperation(
        "MySQL Execute",
        "success",
        Some(s"Executed query: $query")
      )
      
      result
    } match {
      case Success(result) =>
        metrics.stopTimer("mysql_execute")
        result
      case Failure(e) =>
        LogUtils.logOperation(
          "MySQL Execute",
          "failure",
          Some(s"Failed to execute query: ${e.getMessage}")
        )
        throw e
    }
  }

  /**
   * Close all connections
   */
  def closeAll(): Unit = {
    mysqlConnection.foreach { conn =>
      Try {
        if (!conn.isClosed) {
          conn.close()
          LogUtils.logOperation("MySQL Connection", "success", Some("Closed MySQL connection"))
        }
      } match {
        case Failure(e) =>
          LogUtils.logOperation(
            "MySQL Connection",
            "failure",
            Some(s"Failed to close MySQL connection: ${e.getMessage}")
          )
        case Success(_) => // Already logged
      }
    }
    mysqlConnection = None
  }

  /**
   * Register shutdown hook
   */
  sys.addShutdownHook {
    closeAll()
  }
} 