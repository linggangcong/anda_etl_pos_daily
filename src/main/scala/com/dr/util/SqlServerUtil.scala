package com.dr.etl.util

import java.sql.{DriverManager, ResultSet}


/**
  * sqlServer数据库的连接帮助类
  */
object SqlServerUtil {
  private final val SQL_SERVER_CONN_URL = PropertiesUtil.getValueFromConf("sqlserver.connect.url")

  def insert(sql: String, params: Array[String]): Int = {
    var res = 0
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance()
    val conn = DriverManager.getConnection(SQL_SERVER_CONN_URL)
    try {
      // Configure to be Read Only
      val statement = conn.prepareStatement(sql)
      for (i <- 0 to params.length - 1) {
        val j = i + 1
        statement.setString(j, params(i))
      }
      res = statement.executeUpdate()
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
      }
    } finally {
      conn.close()
    }
    res
  }

  def query(sql: String, colName: String): List[String] = {
    //classOf[com.mysql.jdbc.Driver]
    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
    val conn = DriverManager.getConnection(SQL_SERVER_CONN_URL)
    var resultList: List[String] = List()
    // println("hello")
    try {
      // Configure to be Read Only
      val statement = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      // Execute Query
      val rs: ResultSet = statement.executeQuery(sql)

      // Iterate Over ResultSet
      while (rs.next) {
        resultList = resultList ++ List(rs.getString(colName))
        println(rs.getString(colName))
      }

    }
    catch {
      case _: Exception => println("===>")
    }
    finally {
      conn.close
    }
    resultList
  }
}
