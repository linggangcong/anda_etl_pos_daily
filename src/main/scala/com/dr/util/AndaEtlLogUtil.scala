package com.dr.util

import com.dr.model.{EtlLogModel, LogLevelModel, LogTypeModel, PropertiesValueModel}

/**
  * etl 日志管理工具
  */
object AndaEtlLogUtil {
  private final val LOG_TO_SQL_SERVER_ON_OR_OFF = PropertiesValueModel.getMyjLogToSqlServerOpenOrNot()


  /**
    * 美宜佳日志信息生成并插入到SQL server数据库中
    *
    * @param dataDay            美宜佳数据产生得日期，如2018-01-01
    * @param msg                具体需要反馈得日志信息
    * @param logLevel           控制日志输出得日志级别
    * @param logType            日志输出的类型
    * @param logToSqlServerOpen 日志输出到SQL server的开关是否开启，如果关闭，不输出日志
    *                           否则输出日志。此开关防止SQL server坏掉，控制此函数的执行与否
    */
  def myjEtlLogProduce(dataDay: String, msg: String, logLevel: String, logType: String,
                       logToSqlServerOpen: Boolean = LOG_TO_SQL_SERVER_ON_OR_OFF): Unit = {
    if (logToSqlServerOpen) {   //开启功能。
      val etlLogModel: EtlLogModel = EtlLogModel(dataDay, msg, _banner = "R10003",
        _logType = logType, _logLevel = logLevel)

      val sql = "insert into leo_etl_log(data_day,banner,msg,log_level,timestamp,host_address,hostname,log_type)" +
        s"values(?,?,?,?,?,?,?,?)"
      val params = Array[String](etlLogModel.day, etlLogModel.banner, etlLogModel.msg, etlLogModel.logLevel,
        etlLogModel.timestamp, etlLogModel.IP, etlLogModel.hostname, etlLogModel.logType)
      SqlServerUtil.insert(sql, params)
    }
  }

  /**
    * 美宜佳etl处理过程中产生success级别的日志
    *
    * @param dataDay
    * @param msg
    */
  def produceEtlMyjSuccessLog(dataDay: String, msg: String): Unit = {
    myjEtlLogProduce(dataDay, msg, LogLevelModel.SUCCESS_LOG, LogTypeModel.ETL_LOG)
  }

  def produceEtlMyjInfoLog(dataDay: String, msg: String): Unit = {
    myjEtlLogProduce(dataDay, msg, LogLevelModel.INFO_LOG, LogTypeModel.ETL_LOG)
  }

  def produceEtlMyjWarnLog(dataDay: String, msg: String): Unit = {
    myjEtlLogProduce(dataDay, msg, LogLevelModel.WARN_LOG, LogTypeModel.ETL_LOG)
  }

  def produceEtlMyjErrorLog(dataDay: String, msg: String): Unit = {
    myjEtlLogProduce(dataDay, msg, LogLevelModel.ERROR_LOG, LogTypeModel.ETL_LOG)
  }
}
