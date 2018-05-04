package com.dr.work

import java.text.SimpleDateFormat
import java.util.Calendar

import com.dr.banner.posProductProcessor
import com.dr.util.{LoadDataUtil, TimeUtil}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}

import scala.collection.mutable.ArrayBuffer

/**
  * 处理历史交易数据（日期时间段）到流水A表 ，不涉及促销表的更新，关联代码要删除。
  */
object HistoryDataToA_DateRange_main {
  val logger = Logger.getLogger(HistoryDataToA.getClass)

  def main(args: Array[String]): Unit = {
    System.out.println("开始重新刷入需要修复的数据。。。")
    var startDate: String = ""
    var endDate: String = ""
    if (args.length != 2) {
      logger.error("请输入“开始日期  结束日期 格式为：yyyyMMdd")
      System.exit(1)
    } else {
      startDate = args(0)
      endDate = args(1)
    }


    //val inputPath = args(0)  //输入数据文件夹路径固定：hdfs://malogic/usr/samgao/input/anda/

    val banner_code = "R10003"
    //初始化SparkSession
    val spark = SparkSession
      .builder()
      .appName("anda_etl_pos_history_job")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext


    /*//获取输入路径的日期字符串。
    val   cal   =   Calendar.getInstance()
    cal.add(Calendar.DATE,   -1)
    val yesterdayDirectory = new SimpleDateFormat( "yyyyMMdd").format(cal.getTime())*/

    //获取日期范围内全部文件夹名称。
    val date_list = TimeUtil.getDateListStartAndEnd(startDate, endDate)

    if (date_list == null || date_list.length == 0) {
      logger.error("日期过滤条件设置不正确！")
      System.exit(1)
    }

    var posProductRDD : RDD[(String ,String)]= null
    var posMoneyRDD:RDD[(String ,String)]=null
    //var promotionRDD :RDD[(String ,String)]=null
    //对List[String] 日期列表遍历，加载数据文件，并改编码。
    for (date <- date_list) {
      logger.info(date)
      //val inputPathPrifix ="hdfs://malogic/usr/samgao/input/anda/" + date    //测试使用20180424。 yesterdayDirectory
      val inputPathPrifix ="hdfs://192.168.0.151:9000/usr/samgao/input/anda/" + date                 //yesterdayDirectory

      posProductRDD= LoadDataUtil.loadFileToRdd(sc, inputPathPrifix+"/实物流水*", "GBK")  //返回RDD[(String ,String)] 实物流水数据  hdfs://192.168.0.151:9000/user/root/input/anda_pos/201[5678]*/实物*

      posMoneyRDD = LoadDataUtil.loadFileToRdd(sc, inputPathPrifix+"/金额流水*", "GBK")    //金额流水数据  hdfs://192.168.0.151:9000/user/root/input/anda_pos/201[5678]*/金额*

      //promotionRDD = LoadDataUtil.loadFileToRdd(sc,inputPathPrifix+"/*/", "GBK")      //读取所有的文件
      //promotionRDD.take(100).foreach(x => prinltn(x))
    }





    //RDD清洗，转化，返回实物流水数据DataFrame.
    val buy_productpos_df = posProductProcessor.etlProductPosOperation(posProductRDD, spark, banner_code)

    //RDD清洗，转化，返回DataFrame.RDD[Row]
    val buy_moneypos_rdd = posProductProcessor.etlMoneyPosOperation(posMoneyRDD, spark, banner_code)
    //转化金额流水RDD，为DataFrame
    val buy_moneypos_schema:StructType = StructType(Array(
      StructField("flow_no_m",StringType),
      StructField("pay_type",StringType),
      StructField("is_useful",IntegerType)
    ))
    val buy_moneypos_df = spark.createDataFrame(buy_moneypos_rdd ,buy_moneypos_schema)


    //ba_model.dim_gid_drid_rel ,为获取bar_code,从hive表提供的DF。
    val dr_goods_df = spark.table("ba_model.dim_gid_drid_rel")
      .where(s"banner_code='${banner_code}'")
      .select("bar_code", "gid")       //用了leo的，不是我需要的字段。

    //  三个DF。联表，生成A表。
    val original_sale_detail = buy_productpos_df.join(buy_moneypos_df,
      buy_productpos_df("flow_no") === buy_moneypos_df("flow_no_m"), "left")

    .join(dr_goods_df, buy_productpos_df("good_code") === dr_goods_df("gid"), "left")
    .select("bar_code", "flow_no", "banner_code", "retailer_shop_code", "good_code", "trade_date_time",
      "trade_date_timestamp", "card_code", "quantity", "price", "amount", "pay_type","is_useful", "day", "banner")

    //保存添加到A表。
    original_sale_detail.write.mode(SaveMode.Append)
      //.option("path", "/etl/original_sale_detail_leo")
      .format("parquet")
      .partitionBy("day", "banner")
      .saveAsTable("ba_model.original_sale_detail")
    logger.info("the original buy data load successfully!")


    /*//清洗促销数据，返回RDD[Row]
    val promotion_df = posProductProcessor.etlPromotionOperation(promotionRDD, spark, banner_code)
    //promotion_df.collect().foreach(println)  //没有输出，test
    //保存促销清洗后的数据。
    promotion_df.write.mode(SaveMode.Overwrite)   //保存添加到A表。
      //.option("path", "/etl/original_sale_detail_leo")
      //.format("textfile")
      .partitionBy("promotion_banner_code")
      .saveAsTable("ba_model.banner_promotion")
    logger.info("banner_promotion data load successfully!")*/

    sc.stop()
  }
}
