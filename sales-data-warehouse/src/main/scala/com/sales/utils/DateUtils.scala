package com.sales.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.time.format.DateTimeFormatter
import scala.util.{Try, Success, Failure}
import org.apache.log4j.Logger

/**
 * 日期工具类
 * 提供日期格式化、解析、计算等工具方法
 */
object DateUtils {
  private val logger = Logger.getLogger(this.getClass)
  
  // 常用日期格式
  private val DATE_FORMAT = "yyyyMMdd"
  private val DATE_FORMAT_DASH = "yyyy-MM-dd"
  private val DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss"
  
  /**
   * 获取当前日期(yyyyMMdd格式)
   * @return 当前日期字符串
   */
  def getCurrentDate(): String = {
    val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
    LocalDate.now().format(formatter)
  }
  
  /**
   * 获取昨天日期(yyyyMMdd格式)
   * @return 昨天日期字符串
   */
  def getYesterdayDate(): String = {
    val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
    LocalDate.now().minusDays(1).format(formatter)
  }
  
  /**
   * 获取当前时间(yyyy-MM-dd HH:mm:ss格式)
   * @return 当前时间字符串
   */
  def getCurrentDateTime(): String = {
    val formatter = DateTimeFormatter.ofPattern(DATETIME_FORMAT)
    LocalDateTime.now().format(formatter)
  }
  
  /**
   * 格式化日期为yyyy-MM-dd
   * @param dateStr yyyyMMdd格式的日期字符串
   * @return yyyy-MM-dd格式的日期字符串
   */
  def formatDate(dateStr: String): String = {
    try {
      val inputFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val outputFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT_DASH)
      
      val date = LocalDate.parse(dateStr, inputFormatter)
      date.format(outputFormatter)
    } catch {
      case e: Exception =>
        logger.error(s"格式化日期失败: $dateStr, ${e.getMessage}", e)
        dateStr
    }
  }
  
  /**
   * 解析yyyy-MM-dd为yyyyMMdd
   * @param dateStr yyyy-MM-dd格式的日期字符串
   * @return yyyyMMdd格式的日期字符串
   */
  def parseDate(dateStr: String): String = {
    try {
      val inputFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT_DASH)
      val outputFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      
      val date = LocalDate.parse(dateStr, inputFormatter)
      date.format(outputFormatter)
    } catch {
      case e: Exception =>
        logger.error(s"解析日期失败: $dateStr, ${e.getMessage}", e)
        dateStr
    }
  }
  
  /**
   * 日期加减天数
   * @param dateStr yyyyMMdd格式的日期字符串
   * @param days 要加减的天数(正数为加，负数为减)
   * @return 计算后的yyyyMMdd格式日期字符串
   */
  def addDays(dateStr: String, days: Int): String = {
    try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val date = LocalDate.parse(dateStr, formatter)
      val resultDate = date.plusDays(days)
      resultDate.format(formatter)
    } catch {
      case e: Exception =>
        logger.error(s"日期加减天数失败: $dateStr, days=$days, ${e.getMessage}", e)
        dateStr
    }
  }
  
  /**
   * 计算两个日期之间的天数差
   * @param startDateStr 开始日期(yyyyMMdd格式)
   * @param endDateStr 结束日期(yyyyMMdd格式)
   * @return 天数差(结束日期 - 开始日期)
   */
  def daysBetween(startDateStr: String, endDateStr: String): Int = {
    try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val startDate = LocalDate.parse(startDateStr, formatter)
      val endDate = LocalDate.parse(endDateStr, formatter)
      
      java.time.Period.between(startDate, endDate).getDays
    } catch {
      case e: Exception =>
        logger.error(s"计算日期差异失败: $startDateStr 至 $endDateStr, ${e.getMessage}", e)
        0
    }
  }
  
  /**
   * 获取指定日期所在月的第一天
   * @param dateStr yyyyMMdd格式的日期字符串
   * @return 月第一天的yyyyMMdd格式日期字符串
   */
  def getMonthFirstDay(dateStr: String): String = {
    try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val date = LocalDate.parse(dateStr, formatter)
      val firstDayOfMonth = date.withDayOfMonth(1)
      firstDayOfMonth.format(formatter)
    } catch {
      case e: Exception =>
        logger.error(s"获取月第一天失败: $dateStr, ${e.getMessage}", e)
        dateStr
    }
  }
  
  /**
   * 获取指定日期所在月的最后一天
   * @param dateStr yyyyMMdd格式的日期字符串
   * @return 月最后一天的yyyyMMdd格式日期字符串
   */
  def getMonthLastDay(dateStr: String): String = {
    try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val date = LocalDate.parse(dateStr, formatter)
      val lastDayOfMonth = date.withDayOfMonth(date.lengthOfMonth())
      lastDayOfMonth.format(formatter)
    } catch {
      case e: Exception =>
        logger.error(s"获取月最后一天失败: $dateStr, ${e.getMessage}", e)
        dateStr
    }
  }
  
  /**
   * 判断日期字符串是否为有效日期
   * @param dateStr yyyyMMdd格式的日期字符串
   * @return 是否有效
   */
  def isValidDate(dateStr: String): Boolean = {
    Try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      LocalDate.parse(dateStr, formatter)
      true
    } match {
      case Success(_) => true
      case Failure(_) => false
    }
  }
  
  /**
   * 获取过去N天的日期列表(包含今天)
   * @param days 天数
   * @return yyyyMMdd格式的日期列表
   */
  def getLastNDays(days: Int): List[String] = {
    val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
    val today = LocalDate.now()
    
    (0 until days).map { i =>
      today.minusDays(i).format(formatter)
    }.toList
  }
  
  /**
   * 获取日期范围
   * @param startDateStr 开始日期(yyyyMMdd格式)
   * @param endDateStr 结束日期(yyyyMMdd格式)
   * @return yyyyMMdd格式的日期列表
   */
  def getDateRange(startDateStr: String, endDateStr: String): List[String] = {
    try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val startDate = LocalDate.parse(startDateStr, formatter)
      val endDate = LocalDate.parse(endDateStr, formatter)
      
      val daysBetween = java.time.Period.between(startDate, endDate).getDays
      
      if (daysBetween < 0) {
        logger.warn(s"开始日期 $startDateStr 大于结束日期 $endDateStr, 返回空列表")
        List.empty[String]
      } else {
        (0 to daysBetween).map { i =>
          startDate.plusDays(i).format(formatter)
        }.toList
      }
    } catch {
      case e: Exception =>
        logger.error(s"获取日期范围失败: $startDateStr 至 $endDateStr, ${e.getMessage}", e)
        List.empty[String]
    }
  }
  
  /**
   * 获取过期的分区日期列表
   * @param currentDate 当前日期(yyyyMMdd格式)
   * @param retentionDays 保留天数
   * @return 过期的分区日期列表(yyyy-MM-dd格式)
   */
  def getExpiredPartitions(currentDate: String, retentionDays: Int): List[String] = {
    try {
      val formatter = DateTimeFormatter.ofPattern(DATE_FORMAT)
      val current = LocalDate.parse(currentDate, formatter)
      
      // 计算过期日期(当前日期减去保留天数)
      val expiredDate = current.minusDays(retentionDays)
      
      // 获取从很早以前到过期日期的所有日期
      val veryOldDate = LocalDate.of(2020, 1, 1) // 假设2020-01-01为最早日期
      
      val outputFormatter = DateTimeFormatter.ofPattern(DATE_FORMAT_DASH)
      
      // 生成从很早以前到过期日期的所有日期
      var date = veryOldDate
      var result = List.empty[String]
      
      while (!date.isAfter(expiredDate)) {
        result = date.format(outputFormatter) :: result
        date = date.plusDays(1)
      }
      
      result.reverse
    } catch {
      case e: Exception =>
        logger.error(s"获取过期分区失败: currentDate=$currentDate, retentionDays=$retentionDays, ${e.getMessage}", e)
        List.empty[String]
    }
  }
}