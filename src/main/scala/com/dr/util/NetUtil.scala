package com.dr.util

/**
  * 网络工具类
  */
object NetUtil {
  def getHostAddress(): String = {
    var ip = "未知"
    try {
      import java.net.InetAddress
      val address = InetAddress.getLocalHost
      ip = address.getHostAddress
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    ip
  }

  def getHostName(): String = {
    var hostname = "未知"
    try {
      import java.net.InetAddress
      val address = InetAddress.getLocalHost
      hostname = address.getHostName
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
    hostname
  }
}
