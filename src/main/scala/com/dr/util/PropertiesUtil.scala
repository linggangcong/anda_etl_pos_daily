package com.dr.util

import java.util.Properties

import org.apache.log4j.Logger

object PropertiesUtil {
  //val logger = Logger.getLogger(PropertiesUtil.getClass)
  /**
    * 加载properties文件
    *
    * @param confFileName 配置文件名字
    * @return
    */
  private def loadConf(confFileName: String) = {
    val properties = new Properties()
    try {
      val loader = Thread.currentThread().getContextClassLoader
      val is = loader.getResourceAsStream(confFileName)
      properties.load(is)
     //logger.info("成功加载主配置文件！")
    } catch {
      case e: Exception => {
        //logger.error(s"加载配置文件出错：${confFileName}")
        System.exit(0)
      }
    }
    properties
  }

  /**
    * 根据配置文件中的key获得其对应的值
    * @param confKey
    * @return
    */
  def getValueFromConf(confKey: String) = {
    val properties = loadConf("dr_conf.properties")
    properties.getProperty(confKey).trim
  }
}
