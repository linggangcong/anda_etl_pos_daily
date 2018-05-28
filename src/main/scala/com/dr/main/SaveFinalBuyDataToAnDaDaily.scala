/*
package com.dr.main

import com.dr.util.TimeUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SaveFinalBuyDataToAnDaDaily {    //提取hive内的数据，生成DataFrame.  把安达每天的数据从A表导出到B表。
   val logger = Logger.getLogger(SaveFinalBuyDataToAnDaDaily.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("etl an da history of data")
//    .setMaster("local[5]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    var startDate: String = ""
    var endDate: String = ""
    if (args.length != 3) {    //3个参数
      logger.error("没有设置零售商Id和日期过滤条件！")
      System.exit(1)
    } else {

      startDate = args(1)   //格式：2018-05-07
      endDate = args(2)
    }
    val banner_code = args(0)

    /*//删除分区数据，重刷最终流水
    val original_sale_detail_df: DataFrame = hiveContext.table("ba_model.original_sale_detail")   //hive  on spark .
      .where(s"banner='${banner_code}' and is_useful=1")
      .filter(s"day between '${startDate}' and '${endDate}'")*/


    val date_list = TimeUtil.getDateListStartAndEnd(startDate, endDate)  //格式问题
    if (date_list == null || date_list.length == 0) {
      logger.error("日期过滤条件设置不正确！")
      System.exit(1)
    }
    val partitions = ArrayBuffer[String]()
    for (date <- date_list) {
      val partition = s"day='${date}',banner='${banner_code}'"
      partitions.append(partition)
    }
    //删除需要重写的分区数据
    //HiveTableUtil.dropBuyDataOfPartition(hiveContext, "ba_model", "sale_transaction_detail", partitions)


    val dr_goods_df = hiveContext.table("ba_model.dim_gid_drid_rel")
      .where(s"banner_code='${banner_code}' ")
      .select("product_code", "gid")

    val dr_shop_df = hiveContext.table("ba_model.dim_shop")   //获取联表的字段值，生成RDD。
      .where(s"banner_id='${banner_code}'")
      .select("shop_code", "banner_shop_code")

    val dr_promotion_df = hiveContext.table("ba_model.banner_promotion")
      .where(s"promotion_banner='${banner_code}'")
      .select("promotion_code", "promotion_good_id", "promotion_shop_id",
        "promotion_type", "promotion_start", "promotion_end")

    import org.apache.spark.sql.functions.udf

    val get_promotion = udf(getPromotion _)


    val final_sale_detail = original_sale_detail_df.join(dr_goods_df,
      original_sale_detail_df("good_code") === dr_goods_df("gid"), "left")
      .join(dr_shop_df, original_sale_detail_df("retailer_shop_code")
        === dr_shop_df("banner_shop_code"), "left")                           //左连接， original_sale_detail_df    dr_shop_df   dr_goods_df

      .join(dr_promotion_df, original_sale_detail_df("good_code") === dr_promotion_df("promotion_good_id") and    //dr_promotion_df  多个条件连接
        original_sale_detail_df("retailer_shop_code") === dr_promotion_df("promotion_shop_id")
        and original_sale_detail_df("trade_date_timestamp") >= dr_promotion_df("promotion_start") and
        original_sale_detail_df("trade_date_timestamp") <= dr_promotion_df("promotion_end"), "left")
      .withColumn("promotion", get_promotion(dr_promotion_df("promotion_type")))

      .select("product_code", "bar_code", "flow_no", "banner_code", "shop_code",
        "retailer_shop_code", "good_code", "trade_date_time", "card_code", "quantity",
        "price", "amount", "pay_type", "promotion", "promotion_code", "promotion_type", "day", "banner")


    final_sale_detail.write.mode(SaveMode.Append)   //   保存最终表 insert  into sale_transaction_detail 分区表  parquet
      //.option("path", "/etl/sale_transaction_detail_leo")
      .format("parquet")
      .partitionBy("day", "banner")
      .saveAsTable("ba_model.sale_transaction_detail")
    logger.info("the final buy data have loaded in sale_transaction_detail successfully!")
    sc.stop()
  }

  /**
    * 判断是否促销
    *
    * @param in_promotion
    */
  def getPromotion(in_promotion: String) = {
    if (in_promotion == null || in_promotion.trim.length == 0) {
      0
    } else {
      1
    }
  }
}
*/
