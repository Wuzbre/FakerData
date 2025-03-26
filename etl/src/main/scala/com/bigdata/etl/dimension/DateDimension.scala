package com.bigdata.etl.dimension

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.time.{LocalDate, DayOfWeek}
import java.time.format.DateTimeFormatter
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * 日期维度处理类
 * 生成从指定开始日期到结束日期的所有日期记录
 */
class DateDimension(spark: SparkSession) {
  import spark.implicits._

  // 判断是否为工作日
  private def isWorkday(date: LocalDate): Boolean = {
    val dayOfWeek = date.getDayOfWeek
    !(dayOfWeek == DayOfWeek.SATURDAY || dayOfWeek == DayOfWeek.SUNDAY)
  }

  // 判断是否为节假日（简化版本，实际项目中可以通过配置文件或数据库来管理节假日）
  private def isHoliday(date: LocalDate): Boolean = {
    // 这里简单举例几个节日，实际项目中应该从配置或数据库中读取
    val holidays = Set(
      LocalDate.of(2023, 1, 1),   // 元旦
      LocalDate.of(2023, 1, 21),  // 春节前一天
      LocalDate.of(2023, 1, 22),  // 春节
      LocalDate.of(2023, 1, 23),  // 春节
      LocalDate.of(2023, 1, 24),  // 春节
      LocalDate.of(2023, 1, 25),  // 春节
      LocalDate.of(2023, 1, 26),  // 春节
      LocalDate.of(2023, 1, 27),  // 春节
      LocalDate.of(2023, 4, 5),   // 清明节
      LocalDate.of(2023, 5, 1),   // 劳动节
      LocalDate.of(2023, 6, 22),  // 端午节
      LocalDate.of(2023, 9, 29),  // 中秋节
      LocalDate.of(2023, 10, 1)   // 国庆节
    )
    holidays.contains(date)
  }

  // 获取季节
  private def getSeason(month: Int): String = month match {
    case m if m >= 3 && m <= 5 => "春季"
    case m if m >= 6 && m <= 8 => "夏季"
    case m if m >= 9 && m <= 11 => "秋季"
    case _ => "冬季"
  }

  /**
   * 生成日期维度数据
   * @param startDate 开始日期 (格式：yyyy-MM-dd)
   * @param endDate 结束日期 (格式：yyyy-MM-dd)
   * @return 日期维度DataFrame
   */
  def generateDateDim(startDate: String, endDate: String): DataFrame = {
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val start = LocalDate.parse(startDate, formatter)
    val end = LocalDate.parse(endDate, formatter)
    
    // 生成日期序列
    val dateList = Iterator.iterate(start)(_.plusDays(1))
      .takeWhile(!_.isAfter(end))
      .map { date =>
        val year = date.getYear
        val month = date.getMonthValue
        val day = date.getDayValue
        val quarter = (month - 1) / 3 + 1
        val weekOfYear = date.get(java.time.temporal.WeekFields.ISO.weekOfWeekBasedYear())
        val dayOfWeek = date.getDayOfWeek.getValue
        
        (
          date.format(formatter), // date_id
          year,                   // year
          month,                  // month
          day,                    // day
          quarter,                // quarter
          weekOfYear,            // week_of_year
          dayOfWeek,             // day_of_week
          date.getDayOfWeek.toString, // day_name
          date.getMonth.toString,     // month_name
          getSeason(month),           // season
          isWorkday(date),           // is_workday
          isHoliday(date),          // is_holiday
          s"$year-$month",          // year_month
          s"$year-Q$quarter",       // year_quarter
          date.format(formatter)     // dt
        )
      }.toSeq

    // 转换为DataFrame
    spark.createDataFrame(dateList).toDF(
      "date_id",
      "year",
      "month",
      "day",
      "quarter",
      "week_of_year",
      "day_of_week",
      "day_name",
      "month_name",
      "season",
      "is_workday",
      "is_holiday",
      "year_month",
      "year_quarter",
      "dt"
    )
  }

  /**
   * 处理日期维度数据
   * 包括生成数据和写入Hive表
   */
  def processDimDate(): Unit = {
    try {
      // 生成2023年全年的日期维度数据
      val dateDf = generateDateDim("2023-01-01", "2023-12-31")

      // 写入Hive表
      dateDf.write
        .format("hive")
        .mode("overwrite")
        .partitionBy("dt")
        .saveAsTable("dim.dim_date")

      println("日期维度数据处理完成")
    } catch {
      case e: Exception =>
        println(s"日期维度数据处理失败: ${e.getMessage}")
        throw e
    }
  }
} 