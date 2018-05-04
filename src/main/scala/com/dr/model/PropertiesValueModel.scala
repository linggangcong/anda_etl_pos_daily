package com.dr.etl.model

import com.dr.etl.util.PropertiesUtil

/**
  * properties文件中键值对，值得数据模型类
  */
object PropertiesValueModel {
  /**
    * 获取美宜佳etl流程中日志是否输出到sql server。
    * 如果配置错误，默认返回false
    *
    * @return true or false
    */
  def getMyjLogToSqlServerOpenOrNot(): Boolean = {
    try {
      val switch = PropertiesUtil.getValueFromConf("myj.etl.log.to.sql_server").toBoolean
      switch
    } catch {
      case e: Exception => false
    }
  }
}
