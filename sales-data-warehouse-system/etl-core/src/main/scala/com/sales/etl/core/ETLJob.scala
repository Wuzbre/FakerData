package com.sales.etl.core

import org.apache.spark.sql.SparkSession

trait ETLJob {
  def execute(spark: SparkSession, date: String): Unit
  def validate(): Boolean
  def name: String
}

abstract class BaseETLJob extends ETLJob {
  override def validate(): Boolean = true
  
  protected def getCurrentDate: String = {
    java.time.LocalDate.now().toString
  }
  
  protected def getYesterday: String = {
    java.time.LocalDate.now().minusDays(1).toString
  }
} 