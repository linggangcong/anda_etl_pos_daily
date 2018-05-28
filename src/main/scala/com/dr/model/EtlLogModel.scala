package com.dr.model

import com.dr.util.{NetUtil, TimeUtil}

/**
  * etl日志的数据模型
  */
case class EtlLogModel(_day: String, _msg: String, _logType: String,
                       _banner: String, _logLevel: String = "info",
                       _timestamp: String = TimeUtil.getNowDateTimeStamp(),
                       _IP: String = NetUtil.getHostAddress(),
                       _hostname: String = NetUtil.getHostName(),
                      _worker:String="samgao")
{
  val day: String = _day
  val banner = _banner
  val msg = _msg
  val logType = _logType
  val timestamp = _timestamp
  val logLevel = _logLevel
  val IP = _IP
  val hostname = _hostname
  val worker=_worker
}
