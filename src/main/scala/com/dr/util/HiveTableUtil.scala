package com.dr.util

import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

/**
  * 操作hive表
  */
object HiveTableUtil {
  val logger = Logger.getLogger(HiveTableUtil.getClass)

  /**
    * 判断hive中的表是否存在
    *
    * @param hiveContext
    * @param db_name
    * @param tableName
    */
  private def tableIsExists(hiveContext: HiveContext, db_name: String, tableName: String) = {
    var res = false
    try {
      val tables = hiveContext.tableNames(db_name)
      res = tables.contains(tableName)
    } catch {
      case e: Exception => {
        logger.error("leo: the db_name of hive maybe is not exists!")
        e.printStackTrace()
        System.exit(1)
      }
    }
    res
  }

  /**
    * 创建
    *
    * @param hiveContext
    * @param dbName
    * @param tableName
    * @param partitions
    */
  private def createTable(hiveContext: HiveContext, dbName: String, tableName: String,
                          fields: ArrayBuffer[(String, String)],
                          partitions: ArrayBuffer[(String, String)]) = {
    if (fields == null || fields.length == 0) {
      logger.error("there is no fields of the table did you want!")
      System.exit(1)
    } else if (partitions == null || partitions.length == 0) {
      var fields_ = ""
      for (field_type <- fields) {
        println(field_type)
        fields_ += s"${field_type._1} ${field_type._2},"
      }
      fields_ = fields_.substring(0, fields_.length - 1)

      val sql =
        s"""
           |create table ${dbName}.${tableName}(
           |${fields_}
           |)
           |stored as parquet
           |location '/etl/${dbName}/${tableName}'
          """.stripMargin
      hiveContext.sql(sql)
      tableIsExists(hiveContext, dbName, tableName)

    } else {
      var fields_ = ""
      for (field_type <- fields) {
        fields_ += s"${field_type._1} ${field_type._2},"
      }
      fields_ = fields_.substring(0, fields_.length - 1)

      var partitions_ = ""
      for (partition_type <- partitions) {
        partitions_ += s"${partition_type._1} ${partition_type._2},"
      }
      partitions_ = partitions_.substring(0, partitions_.length - 1)
      val sql =
        s"""
           |create table ${dbName}.${tableName}(
           |${fields_}
           |)
           |partitioned by(${partitions_})
           |stored as parquet
           |location '/etl/${dbName}/${tableName}'
          """.stripMargin
      hiveContext.sql(sql)
      tableIsExists(hiveContext, dbName, tableName)
    }
  }

  /**
    *
    * @param dbName
    * @param tableName
    */
  def createOriginalSaleDetailTab(hiveContext: HiveContext, dbName: String, tableName: String) = {
    val fields: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    fields.append(("bar_code", "string"))
    fields.append(("flow_no", "string"))
    fields.append(("banner_code", "string"))
    fields.append(("retailer_shop_code", "string"))
    fields.append(("good_code", "string"))
    fields.append(("trade_date_time", "string"))
    fields.append(("trade_date_timestamp", "bigint"))
    fields.append(("card_code", "string"))
    fields.append(("quantity", "float"))
    fields.append(("price", "float"))
    fields.append(("amount", "float"))
    fields.append(("pay_type", "string"))
    fields.append(("is_useful", "int"))
    val partitions: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    partitions.append(("day", "string"))
    partitions.append(("banner", "string"))
    if (tableIsExists(hiveContext, dbName, tableName) == false) {
      createTable(hiveContext, dbName, tableName, fields, partitions)
    } else {
      logger.info(s"${dbName}.${tableName} is exists!")
      true
    }
  }

  /**
    * 创建最终流水表
    *
    * @param hiveContext
    * @param dbName
    * @param tableName
    */
  def createFinalSaleDetailTab(hiveContext: HiveContext, dbName: String, tableName: String) = {
    val fields: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    fields.append(("product_code", "string"))
    fields.append(("bar_code", "string"))
    fields.append(("flow_no", "string"))
    fields.append(("banner_code", "string"))
    fields.append(("shop_code", "string"))
    fields.append(("retailer_shop_code", "string"))
    fields.append(("good_code", "string"))
    fields.append(("trade_date_time", "string"))
    fields.append(("card_code", "string"))
    fields.append(("quantity", "float"))
    fields.append(("price", "float"))
    fields.append(("amount", "float"))
    fields.append(("pay_type", "string"))
    fields.append(("promotion", "int"))
    fields.append(("promotion_code", "string"))
    fields.append(("promotion_type", "string"))
    val partitions: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
    partitions.append(("day", "string"))
    partitions.append(("banner", "string"))
    if (tableIsExists(hiveContext, dbName, tableName) == false) {
      createTable(hiveContext, dbName, tableName, fields, partitions)
    } else {
      logger.info(s"${dbName}.${tableName} is exists!")
      true
    }
  }

  /**
    * 删除某个库中某个表，指定分区（可以是多分区）的数据
    *
    * @param hiveContext
    * @param db_name
    * @param table_name
    * @param partitions (分区字段，具体分区值）
    */
  def dropBuyDataOfPartition(hiveContext: HiveContext, db_name: String, table_name: String,
                             partitions: ArrayBuffer[String]) = {
    if (tableIsExists(hiveContext, db_name, table_name)) {
      //需要删除分区的表不存在
      true
    }
    else if (partitions == null || partitions.length == 0) {
      logger.error("没有指定要删除的分区！")
      false
    } else {
      try {
        for (partition <- partitions) {
          hiveContext.sql(s"alter table ${db_name}.${table_name} " +
            s"DROP IF EXISTS PARTITION(${partition})")
        }
        true
      }
      catch {
        case e: Exception => {
          logger.error("分区数据删除失败！")
          false
        }
      }
    }
  }
}
