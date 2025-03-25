package com.sales.etl

import java.sql.{Connection, DriverManager, Timestamp}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class MetadataManager(config: Config) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val mysqlConfig = config.getConfig("etl.mysql")
  
  // 初始化元数据表
  def initializeMetadataTables(): Unit = {
    withConnection { conn =>
      val statement = conn.createStatement()
      
      // 创建同步状态跟踪表
      statement.execute("""
        CREATE TABLE IF NOT EXISTS etl_sync_status (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          table_name VARCHAR(100) NOT NULL,
          last_sync_time TIMESTAMP NOT NULL,
          records_processed BIGINT NOT NULL DEFAULT 0,
          status VARCHAR(20) NOT NULL,
          error_message TEXT,
          created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
          update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          UNIQUE KEY uk_table_name (table_name)
        )
      """)
      
      // 创建ETL运行历史表
      statement.execute("""
        CREATE TABLE IF NOT EXISTS etl_job_history (
          id BIGINT AUTO_INCREMENT PRIMARY KEY,
          job_name VARCHAR(100) NOT NULL,
          start_time TIMESTAMP NOT NULL,
          end_time TIMESTAMP,
          status VARCHAR(20) NOT NULL,
          total_records BIGINT,
          success_records BIGINT,
          failed_records BIGINT,
          error_message TEXT,
          created_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
      """)
    }
  }
  
  // 更新同步状态
  def updateSyncStatus(tableName: String, syncTime: Timestamp, recordsProcessed: Long, status: String, errorMessage: Option[String] = None): Unit = {
    withConnection { conn =>
      val sql = """
        INSERT INTO etl_sync_status (table_name, last_sync_time, records_processed, status, error_message)
        VALUES (?, ?, ?, ?, ?)
        ON DUPLICATE KEY UPDATE
          last_sync_time = VALUES(last_sync_time),
          records_processed = records_processed + VALUES(records_processed),
          status = VALUES(status),
          error_message = VALUES(error_message)
      """
      
      val stmt = conn.prepareStatement(sql)
      stmt.setString(1, tableName)
      stmt.setTimestamp(2, syncTime)
      stmt.setLong(3, recordsProcessed)
      stmt.setString(4, status)
      stmt.setString(5, errorMessage.orNull)
      stmt.executeUpdate()
    }
  }
  
  // 获取上次同步时间
  def getLastSyncTime(tableName: String): Option[Timestamp] = {
    withConnection { conn =>
      val sql = "SELECT last_sync_time FROM etl_sync_status WHERE table_name = ? AND status = 'SUCCESS'"
      val stmt = conn.prepareStatement(sql)
      stmt.setString(1, tableName)
      val rs = stmt.executeQuery()
      
      if (rs.next()) {
        Option(rs.getTimestamp("last_sync_time"))
      } else {
        None
      }
    }
  }
  
  // 记录ETL作业运行历史
  def recordJobHistory(jobName: String, startTime: Timestamp, endTime: Timestamp, status: String,
                      totalRecords: Long, successRecords: Long, failedRecords: Long, errorMessage: Option[String] = None): Unit = {
    withConnection { conn =>
      val sql = """
        INSERT INTO etl_job_history 
        (job_name, start_time, end_time, status, total_records, success_records, failed_records, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      """
      
      val stmt = conn.prepareStatement(sql)
      stmt.setString(1, jobName)
      stmt.setTimestamp(2, startTime)
      stmt.setTimestamp(3, endTime)
      stmt.setString(4, status)
      stmt.setLong(5, totalRecords)
      stmt.setLong(6, successRecords)
      stmt.setLong(7, failedRecords)
      stmt.setString(8, errorMessage.orNull)
      stmt.executeUpdate()
    }
  }
  
  // 获取作业运行历史
  def getJobHistory(jobName: String, limit: Int = 10): List[JobHistoryRecord] = {
    withConnection { conn =>
      val sql = """
        SELECT * FROM etl_job_history 
        WHERE job_name = ?
        ORDER BY start_time DESC
        LIMIT ?
      """
      
      val stmt = conn.prepareStatement(sql)
      stmt.setString(1, jobName)
      stmt.setInt(2, limit)
      val rs = stmt.executeQuery()
      
      var records = List[JobHistoryRecord]()
      while (rs.next()) {
        records = JobHistoryRecord(
          rs.getLong("id"),
          rs.getString("job_name"),
          rs.getTimestamp("start_time"),
          Option(rs.getTimestamp("end_time")),
          rs.getString("status"),
          rs.getLong("total_records"),
          rs.getLong("success_records"),
          rs.getLong("failed_records"),
          Option(rs.getString("error_message"))
        ) :: records
      }
      records.reverse
    }
  }
  
  private def withConnection[T](block: Connection => T): T = {
    var conn: Connection = null
    try {
      Class.forName(mysqlConfig.getString("driver"))
      conn = DriverManager.getConnection(
        mysqlConfig.getString("url"),
        mysqlConfig.getString("user"),
        mysqlConfig.getString("password")
      )
      block(conn)
    } finally {
      if (conn != null) {
        try {
          conn.close()
        } catch {
          case e: Exception => logger.error("Error closing connection", e)
        }
      }
    }
  }
}

case class JobHistoryRecord(
  id: Long,
  jobName: String,
  startTime: Timestamp,
  endTime: Option[Timestamp],
  status: String,
  totalRecords: Long,
  successRecords: Long,
  failedRecords: Long,
  errorMessage: Option[String]
) 