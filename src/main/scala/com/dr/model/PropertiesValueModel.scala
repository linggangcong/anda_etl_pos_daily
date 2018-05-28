package com.dr.model

import com.dr.util.PropertiesUtil

/**
  * Created by SAM on 2018/5/16.
  */
object PropertiesValueModel {
  /**
    * 获取anda etl流程中日志是否输出到sql server。
    * 如果配置错误，默认返回false
    *
    * @return true or false
    */
  def getMyjLogToSqlServerOpenOrNot: Boolean = {
    try{
      val switch = PropertiesUtil.getValueFromConf("anda.etl.log.to.sql_server").toBoolean
      switch
    }catch {
      case e: Exception => false
    }

  }
}
