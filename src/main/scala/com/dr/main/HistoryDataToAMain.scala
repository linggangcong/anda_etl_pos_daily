
package com.dr.main

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.dr.banner.posProductProcessor
import com.dr.util.{LoadDataUtil, SqlServerUtil, TimeUtil}
import com.dr.work.{HistoryDataToA_DateRange}
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
/**
  * 处理历史交易数据到流水A表
  */
object HistoryDataToAMain {
  val logger = Logger.getLogger(HistoryDataToAMain.getClass)

  def main(args: Array[String]): Unit = {
    /*if (args.length != 1) {
      logger.error("请输入历史交易数据的路径!")
      System.exit(1)
    }
    val inputPath = args(0)*/
    val banner_code = "R10003"
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val sdf=new SimpleDateFormat("yyyyMMdd")
    val yesterdayDirectory =sdf .format(cal.getTime()) //昨天数据文件夹名 比如20180502

    //最近的错误数据天
    val sqlFindFailure: String = "select data_day from dbo.leo_etl_log where banner='R10003' and log_type='error'"
    val LatestFailureDayList = SqlServerUtil.getLatestDay(sqlFindFailure, "data_day")
    //确定只有一天是错误。
    val latestFailureDay: String = TimeUtil.getLatestDay(LatestFailureDayList)

    //没有错误天，找最近的成功天。
    val sqlFindSuccess: String = "select data_day from dbo.leo_etl_log where banner='R10003' and log_type='success'"
    //最近的成功日期，之后的一天。
    val LatestSuccessDayList = SqlServerUtil.getLatestDay(sqlFindSuccess, "data_day")
    val latestSuccessDay  = TimeUtil.getLatestDay(LatestSuccessDayList)    //20180505
    var latestSuccessDate: Date = null
    if (latestFailureDay != null) {
      //HistoryDataToA_DateRange.etlDateRange(latestFailureDay, yesterdayDirectory)
      HistoryDataToA_DateRange.etlDateRange(latestFailureDay, "20180515")
    }else if (latestSuccessDay != null) {
      try
        latestSuccessDate = sdf.parse(latestSuccessDay)
      catch {
        case e: Exception => {
          e.printStackTrace()
        }
      }
      cal.setTime(latestSuccessDate)
      cal.add(Calendar.DAY_OF_MONTH, +1)
      val Plus1Date: Date = cal.getTime
      val LatestSuccessDayPlus1: String = sdf.format(Plus1Date)
      HistoryDataToA_DateRange.etlDateRange(LatestSuccessDayPlus1, yesterdayDirectory)
    } else System.exit(1)

  }
}

