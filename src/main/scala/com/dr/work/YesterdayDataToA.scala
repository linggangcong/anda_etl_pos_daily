
package com.dr.work

import java.text.SimpleDateFormat
import java.util.Calendar

import com.dr.banner.posProductProcessor
import com.dr.util.LoadDataUtil
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * 处理历史交易数据到流水A表,单纯的处理前一天的数据。
  */
object YesterdayDataToA {
  val logger = Logger.getLogger(YesterdayDataToA.getClass)

  def etlNomal(): Unit = {
    val banner_code = "R10003"
    //初始化SparkSession
    val spark = SparkSession
      .builder()
      .appName("anda_etl_pos_history_job")
     // .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
     // .config("spark.driver.allowMultipleContexts", true)
      .enableHiveSupport()
      .getOrCreate()

    //val conf = new SparkConf().setAppName("anda_etl_pos_history_job").setMaster("local")
      //.setMaster("local[5]")
   // val sc = new SparkContext(conf)
    val sc = spark.sparkContext

    //val sqlContext = new SQLContext(sc)
    //val hiveContext = new HiveContext(sc)    //生成运行环境。

    //获取输入路径的日期字符串。
    val   cal   =   Calendar.getInstance()
    cal.add(Calendar.DATE,   -1)
    val yesterdayDirectory = new SimpleDateFormat( "yyyyMMdd").format(cal.getTime()) //昨天数据文件夹名

    //加载数据文件，并改编码。
    val inputPathPrifix ="hdfs://malogic/usr/samgao/input/anda/"+yesterdayDirectory        // PRODUCT
    //val inputPathPrifix ="hdfs://192.168.0.151:9000/usr/samgao/input/anda/20180424"       //yesterdayDirectory

    val posProductRDD= LoadDataUtil.loadFileToRdd(sc, inputPathPrifix+"/实物流水*", "GBK")  //返回RDD[(String ,String)] 实物流水数据  hdfs://192.168.0.151:9000/user/root/input/anda_pos/201[5678]*/实物*
    val posMoneyRDD = LoadDataUtil.loadFileToRdd(sc, inputPathPrifix+"/金额流水*", "GBK")    //金额流水数据  hdfs://192.168.0.151:9000/user/root/input/anda_pos/201[5678]*/金额*
    val promotionRDD = LoadDataUtil.loadFileToRdd(sc,inputPathPrifix+"/*/", "GBK")      //读取所有的文件
    //promotionRDD.take(100).foreach(x => prinltn(x))


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


    //清洗促销数据，返回RDD[Row]
    val promotion_df = posProductProcessor.etlPromotionOperation(promotionRDD, spark, banner_code)
    //promotion_df.collect().foreach(println)  //没有输出，test
    //保存促销清洗后的数据。
    promotion_df.write.mode(SaveMode.Overwrite)   //保存添加到A表。
      //.option("path", "/etl/original_sale_detail_leo")
      //.format("textfile")
      .partitionBy("promotion_banner_code")
      .saveAsTable("ba_model.banner_promotion")
    logger.info("banner_promotion data load successfully!")

    sc.stop()
  }

}

