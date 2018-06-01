package com.dr.work

import com.dr.banner.posProductProcessor
import com.dr.util.{AndaEtlLogUtil, LoadDataUtil, SqlServerUtil, TimeUtil}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


/**
  * 处理交易数据（日期时间段）到流水A表 ，不涉及促销表的更新，关联代码要删除。
  */
object HistoryDataToA_DateRange {
  val logger = Logger.getLogger(HistoryDataToA_DateRange.getClass)
  def etlDateRange(startDate:String ,endDate:String): Unit = {      //传入参数为包含开始日期，结束日期的字符串数组，日期格式为20180422。
    System.out.println("开始清洗数据任务。。。")

       //val inputPath = args(0)       //输入数据文件夹路径固定：hdfs://malogic/usr/samgao/input/anda/
    val banner_code = "R10003"
    //初始化SparkSession
    val spark = SparkSession
      .builder()
      .appName("anda_etl_pos_history_job")
      .enableHiveSupport()
      .config("hive.exec.dynamic.partition", true)             //支持Hive动态分区
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("fs.defaultFS", "hdfs://malogic")     //变动，环境

      .getOrCreate()
    val sc = spark.sparkContext
    val HCtx=new HiveContext(sc)

    try{
    //获取日期范围内全部文件夹名称。
    val date_list = TimeUtil.getDateListStartAndEnd(startDate, endDate)

    /*if (date_list == null || date_list.isEmpty) {
      logger.error("日期过滤条件设置不正确！")
      AndaEtlLogUtil.produceEtlMyjErrorLog(endDate, "开始和结束日期设置不正确！")   //endDate，表示程序执行日的前一天，也是最后执行的数据日期日
      System.exit(1)
    }*/

    var posProductRDD : RDD[(String ,String)]= null
    var posMoneyRDD:RDD[(String ,String)]=null
    var promotionRDD :RDD[(String ,String)]=null
    var dimProductRDD:RDD[(String ,String)]=null

    //对List[String] 日期列表遍历，加载数据文件，并改编码。

   /* for (date <- date_list) {    //一次导入全部日期段的所有数据。一次RDD处理。
      //logger.info(date)
      val inputPathPrifix ="hdfs://malogic/usr/samgao/input/anda/" + date    //测试使用20180424。 yesterdayDirectory
      //val inputPathPrifix ="hdfs://192.168.0.151:9000/usr/samgao/input/anda/" + date                 //yesterdayDirectory
      posProductRDD= LoadDataUtil.loadFileToRdd(sc, inputPathPrifix+"/实物流水*", "GBK" ,endDate)  //返回RDD[(String ,String)] 实物流水数据
      posMoneyRDD = LoadDataUtil.loadFileToRdd(sc, inputPathPrifix+"/金额流水*", "GBK" ,endDate)    //金额流水数据
    }*/



    //生成路径，逗号分割，包含多个文件。
    var posProductPath="";
    var posMoneyPath="";
    for (date <- date_list) {    //一次导入全部日期段的所有数据。一次RDD处理。
      posProductPath =posProductPath+"hdfs://malogic/usr/samgao/input/anda/" + date +"/实物流水*,"

      posMoneyPath= posMoneyPath +"hdfs://malogic/usr/samgao/input/anda/" + date +"/金额流水*,"
    }
    //去除最后一个逗号
    posProductPath= posProductPath.substring(0,posProductPath.length - 1)
    posMoneyPath= posMoneyPath.substring(0,posMoneyPath.length - 1)

    //加载数据，逗号分割路径
    posProductRDD= LoadDataUtil.loadFileToRdd(sc, posProductPath, "GBK" ,endDate)  //返回RDD[(String ,String)] 实物流水数据
    posMoneyRDD = LoadDataUtil.loadFileToRdd(sc, posMoneyPath, "GBK" ,endDate)    //金额流水数据

    promotionRDD = LoadDataUtil.loadFileToRdd(sc,"hdfs://malogic/usr/samgao/input/anda/" + endDate+"/*/", "GBK",endDate)
    dimProductRDD = LoadDataUtil.loadFileToRdd(sc,"hdfs://malogic/usr/samgao/input/anda/" + endDate+"/商品档案表*", "GBK",endDate)
    //promotionRDD = LoadDataUtil.loadFileToRdd(sc,"hdfs://192.168.0.151:9000/usr/samgao/input/anda/" + endDate+"/*/", "GBK" ,endDate) //TEST
    //dimProductRDD = LoadDataUtil.loadFileToRdd(sc,"hdfs://192.168.0.151:9000/usr/samgao/input/anda/"+ endDate+"/商品档案表*", "GBK",endDate)  //endDate 处理数据的日期。
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

    //商品表处理， 使用商品编码连接，获取条形码。
    val dim_product_rdd = posProductProcessor.etlDimProductOperation(dimProductRDD, spark, banner_code)

    //转化商品档案表RDD，为DataFrame
    val dim_product_schema:StructType = StructType(Array(
      StructField("dim_product_code",StringType),
      StructField("bar_code",StringType)
    ))
    val dim_product_df = spark.createDataFrame(dim_product_rdd,dim_product_schema)


    //  三个DF。联表，生成A表。
    val original_sale_detail_df = buy_productpos_df.join(buy_moneypos_df,
      buy_productpos_df("flow_no") === buy_moneypos_df("flow_no_m"), "left")
    .join(dim_product_df, buy_productpos_df("good_code") === dim_product_df("dim_product_code"), "left")
    .select("bar_code", "flow_no", "banner_code", "retailer_shop_code", "good_code", "trade_date_time",
      "trade_date_timestamp", "card_code", "quantity", "price", "amount", "pay_type","is_useful", "day", "banner")

    //保存添加到A表
    val lineNum =original_sale_detail_df.count()
    AndaEtlLogUtil.produceEtlAndaInfoLog(endDate, s"${startDate}---> ${endDate}实物流水写入Hive的行数：${lineNum}")         // 行数输出
    original_sale_detail_df.createOrReplaceTempView("tempView")                                   //保存到临时表，然后导入A表动态分区。
    HCtx.sql("insert into ba_model.original_sale_detail partition(day ,banner) select * from tempView")   //考虑overwrite写入。避免重复。


    //清洗促销数据，返回RDD[Row]
    val promotion_df = posProductProcessor.etlPromotionOperation(promotionRDD, spark, banner_code)
    //promotion_df.collect().foreach(println)  //没有输出，test
    //保存促销清洗后的数据。
    promotion_df.write.mode(SaveMode.Overwrite)   //保存添加到促销表，有分区。
    .insertInto("ba_model.banner_promotion")
    logger.info("banner_promotion data load successfully!")
    //AndaEtlLogUtil.produceEtlMyjInfoLog(endDate, s"今天的实物流水写入Hive的行数：${lineNum}")
    sc.stop()
    AndaEtlLogUtil.produceEtlAndaSuccessLog(endDate, startDate+"---> " + endDate + "日期段内数据清洗成功")

    //成功，清理所有日志为error的日志记录。
    val params=null  //随意放一个参数数组。
    SqlServerUtil.insert("update leo_etl_log set log_level='success_from_error' where  log_level='error'" , params  )

    }catch {
      case e: Exception => {
        e.printStackTrace()
        AndaEtlLogUtil.produceEtlAndaErrorLog(startDate, startDate+"---> " + endDate + "日期段内数据清洗失败")
        System.exit(1)
      }
    }

    //写入B表
    val original_sale_detail_df: DataFrame = HCtx.table("ba_model.original_sale_detail")    //hive  on spark .
      .where(s"banner='${banner_code}' and is_useful=1")
      .filter(s"day between '${startDate}' and '${endDate}'")

    val dr_goods_df = HCtx.table("ba_model.dim_gid_drid_rel")    // 通过零售商商品code,获取DR商品编码。
      .where(s"banner_code='${banner_code}' ")
      .select("product_code", "gid")

    val dr_shop_df = HCtx.table("ba_model.dim_shop")   // 通过零售商店铺code,获取DR店铺编码。
      .where(s"banner_id='${banner_code}'")
      .select("shop_code", "banner_shop_code")

    val dr_promotion_df = HCtx.table("ba_model.banner_promotion")   //
      .where(s"promotion_banner='${banner_code}'")
      .select("promotion_code", "promotion_good_id", "promotion_shop_id",
        "promotion_type", "promotion_start", "promotion_end")

    import org.apache.spark.sql.functions.udf

    val get_promotion = udf(getPromotion _)


    val final_sale_detail = original_sale_detail_df.join(dr_goods_df,
      original_sale_detail_df("good_code") === dr_goods_df("gid"), "left")
      .join(dr_shop_df, original_sale_detail_df("retailer_shop_code")
        === dr_shop_df("banner_shop_code"), "left")







  }


}
