package com.dr.banner

import com.dr.schema.{BannerBuySchema, promotionHiveTalSchema}
import com.dr.util.TimeUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

/**
  * 零售商流水数据的DataFrame
  */
object posProductProcessor {
  /**
    * 处理流水数据，到A表
    *
    * @param posProductDataRDD
    */
  def etlProductPosOperation(posProductDataRDD: RDD[(String, String)], sqlContext: SparkSession, banner_code: String)= {

    val buy_product_rdd = posProductDataRDD.filter(x => {
      //对输入的RDD，进行处理，主要是处理第二字段，即数据记录。
      if (x._2 == null || x._2.isEmpty) {
        false
      } /*else {
        val splits = x._2.split("/t", -2) //用tab空格分割数据   //对RDD中一个记录，使用空格做分割。splits 是字符串数组。
        for (i <- 0 to splits.length) {
          //统一处理字符串的空值，修剪前后缀空格。
          if (splits(i) == null || splits(i).isEmpty) {
            splits(i) = null
          } else {
            splits(i) = splits(i).trim()
          }
        }*/
      val splits = x._2.split("\t", -2)   //同样这里
      //splits(i) = splits(i).trim()
      if (splits.length <= 12 || splits(0).contains("NNN") || splits(0).contains("流水号") || splits(9).startsWith("-") || splits(11).startsWith("-")|| splits(2).length <20 ) {
        false
      } else {
        true
      }
      }).map(x => {
      val splits = x._2.split("\t", -2)
      //对记录的字段数量（20）检验。并提取需要的字段，进行格式化，包括trim ,截取店铺号，截取商品号，购买日期的格式， 购买日期变成毫秒，单价字符串的获取。
      splits.length match {
        case 20 => s"${splits(0)},${banner_code},${splits(3).substring(0, 4)},${splits(6).replaceAll("^(0+)", "")}," + // ‘s’字符串插值器
          s"${TimeUtil.dateShaped(splits(2))},${TimeUtil.dataToLong(TimeUtil.dateShaped(splits(2)))},${null},${splits(9)}," +
          s"${TimeUtil.priceStr(splits(9), splits(10))},${splits(11)}"     //10个 flow_no  banner_code, retailer_shop_code  good_code, trade_date_time, trade_date_timestamp, card_code, quantity, price, amount, day, banner
        case _ => null
      }

    }).filter(x => {
      val splits = x.split(",",-2)
      x!=null && splits(4)!= null   //过滤购买日期为空。 过滤列数不为20，x为null.

    }).map(x => {
      //这里去除了关于分片路径的字符串信息。
      val splits = x.split(",",-2)
      (splits(0), splits(1), splits(2), splits(3), splits(4), splits(5),
        splits(6), splits(7), splits(8), splits(9))
    })

    //以下为生成DF。
    val row_product_rdd = buy_product_rdd.map(x => {   //元素为row类型的RDD。
        Row(x._1, x._2, x._3, x._4, x._5.replace("/","-"), x._6.toLong, x._7, x._8.toFloat, x._9.toFloat, x._10.toFloat , x._5.substring(0,10).replace("/","-"),"R10003")   //联表之前，还不能保存。14个字段 (bar_code, 付款类型)，缺一个付款类型,isuseful 1。
     }).filter(x => x != null)
    val banner_buy_df = sqlContext.createDataFrame(row_product_rdd, BannerBuySchema.BannerBuy)
    banner_buy_df                    //返回DF。包含14个字段
  }



  def etlMoneyPosOperation(posMoneyPosDataRDD: RDD[(String, String)], sqlContext: SparkSession, banner_code: String) = {

    val buy_money_rdd = posMoneyPosDataRDD.filter(x => {
      //对输入的RDD，进行处理，主要是处理第二字段，即数据记录。
      if (x._2 == null || x._2.isEmpty) {
        false
      }/* else {
        val splits = x._2.split("/t", -2)    //用tab空格分割数据   //对RDD中一个记录，使用空格做分割。splits 是字符串数组。
        for (i <- 0 to splits.length) {
          //统一处理字符串的空值，修剪前后缀空格。
          if (splits(i) == null || splits(i).isEmpty) {
            splits(i) = null
          } else {
            splits(i) = splits(i).trim()
          }*/

      val splits = x._2.split("\t", -2)
      if (splits.length <= 4 || splits(0).contains("NNN") || splits(0).contains("流水号")) {
        false
      } else {
        true
      }



    }).map(x => {
      val splits = x._2.split("\t", -2)
      //对记录的字段数量（20）检验。并提取需要的字段，进行格式化，包括trim ,截取店铺号，截取商品号，购买日期的格式， 购买日期变成毫秒，单价字符串的获取。
      splits.length match {
        case 13 => s"${splits(0)},${splits(3)},1"
        case _ => null
      }
    }).filter(x => x != null).map(x => {
      //这里去除了关于分片路径的字符串信息。
      val splits = x.split(",")
      Row(splits(0), splits(1), "1".toInt)
    })

    buy_money_rdd

    /*//以下为生成DF。
    val row_money_rdd = buy_money_rdd.map(x => {   //元素为row类型的RDD。

      Row(x._1, x._2, x._3)   //联表之前，还不能保存。14个字段 (bar_code, 付款类型)，缺一个付款类型,isuseful 1。
    }).filter(x => x != null)

    val banner_buy_money_df = sqlContext.createDataFrame(row_money_rdd, BannerMoneyPosSchema.BannerMoneyPos)

    banner_buy_money_df    //返回DF。包含三个字段*/
  }

  def etlDimProductOperation(dimProductRDD: RDD[(String, String)], sqlContext: SparkSession, banner_code: String) = {
    val dim_product_rdd = dimProductRDD.filter(x => {
      //对输入的RDD，进行处理，主要是处理第二字段，即数据记录。
      val splits = x._2.split("\t", -2)
      if (splits.length < 4 || splits(0).contains("ABC类别")) {
        false
      } else {
        true
      }
    }).map(x => {
      val splits = x._2.split("\t",-2)
      splits.length match {
        case 78 => s"${splits(1).trim.replaceAll("^(0+)", "")},${splits(3).trim}"   //表一共有79列，包括一个空列。商品编码  条形码
        case _ => null
      }
    }).filter(x => x!=null
    ).filter(x => {
      val splits = x.split(",")
      if(splits.length<2 ){
        false
      }else {
        true
      }
    }).map(x => {
      //这里去除了关于分片路径的字符串信息。
      val splits = x.split(",")
      Row(splits(0), splits(1))
    })
    //dim_product_rdd.collect().foreach(println)
    dim_product_rdd
  }




  //促销清洗过程。
  def etlPromotionOperation(posPromotionDataRDD: RDD[(String, String)], sqlSession: SparkSession, banner_code: String) = {   //输入数据RDD，RDD[(String, String)]，

    val promotion_rdd = posPromotionDataRDD.filter(x => {      //可以在这里对x._1 进行判断。
      //对输入的RDD，进行处理，主要是处理第二字段，即数据记录。
      if (x._2 == null || x._2.isEmpty){
        false
      }else{
        val splits = x._2.split("\t", -2)
        if ( splits(0).contains("促销单号") || splits(0).contains("单据号")|| splits.length < 5 ) {
          false
        } else {
          true
        }
      }

        /*val splits = x._2.split("\t", -2)   //用tab空格分割数据   //对RDD中一个记录，使用空格做分割。splits 是字符串数组。
        for (i <- 0 to splits.length) {
          //统一处理字符串的空值，修剪前后缀空格。
          if (splits(i) == null || splits(i).isEmpty) {
            splits(i) = null
          } else {
            splits(i) = splits(i).trim()
          }*/

    //RDD[(String, String)]
    }).map(x => {
      val splits = x._2.split("\t", -2)      //问题在这里,写出/t。
      //对记录的字段数量（20）检验。并提取需要的字段，进行格式化，包括trim ,截取店铺号，截取商品号，购买日期的格式， 购买日期变成毫秒，单价字符串的获取。
      /*for (i <- 0 to splits.length) {
        //统一处理字符串的空值，修剪前后缀空格。
        if (splits(i) == null || splits(i).isEmpty) {
          splits(i) = null
        } else {
          splits(i) = splits(i).trim()
        }
      }*/

      if(x._1.contains("进价")){
        if(splits.length<30 ){
          null
        }else {
          s"${null},${splits(3).replaceAll("^(0+)", "").trim},${splits(6).trim},进价售价促销,${TimeUtil.promotionPriceDateArr(splits(10),splits(11),splits(12),splits(13),splits(28),splits(29))(0)}" +
            s",${TimeUtil.promotionPriceDateArr(splits(10),splits(11),splits(12),splits(13),splits(28),splits(29))(1)},R10003,R10003"     //日期时long.其他需要trim.
        }
      }else  if (x._1.contains("组合优惠") ) {
        if(splits.length<26 ||splits(5) .startsWith("G") ) {
          null
        }else{
          s"${null},${splits(5).replaceAll("^(0+)", "").trim},${splits(2).trim},组合优惠促销," + s"${this.startEndDate(splits,22,24,23,25)(0)}," +
            s"${this.startEndDate(splits,22,24,23,25)(1)}" + s",R10003,R10003"
        }

      /*}else if (x._1.contains("商品总额") ){
        if(splits.length<20 ||splits(16).length<15 || splits(17).length<15) {
          null
        }else{
          s"${null},${splits(7).replaceAll("^(0+)", "")},${splits(4)},商品总额优惠," + s"${this.startEndDate(splits,16,18,17,19)(0)}," +
            s"${this.startEndDate(splits,16,18,17,19)(1)} " + s" ,R10003,R10003"
        }*/
      }else if(x._1.contains("全场总额")){
        if(splits.length<16 ||splits(12).length<15 || splits(13).length<15) {
          null
        }else{
          s"${null},${splits(8).replaceAll("^(0+)", "").trim},${splits(5).trim},全场总额优惠," + s"${this.startEndDate(splits,12,14,13,15)(0)}," +
            s"${this.startEndDate(splits,12,14,13,15)(1)}" + s",R10003,R10003"
        }
      }else if (x._1.contains("等级优惠")){
        if(splits.length<26 ||splits(22).length<11 || splits(23).length<11){
          null
        } else {
          s"${null},${splits(8).replaceAll("^(0+)", "").trim},${splits(5).trim},等级促销优惠," + s"${this.startEndDate(splits,22,24,23,25)(0)}," +
            s"${this.startEndDate(splits,22,24,23,25)(1)}" + s",R10003,R10003"
        }

      }else{
        null
      }


      /*x._1 match {
        case 48 => s"${null},${splits(3)},${splits(6)},进价售价促销,${TimeUtil.promotionPriceDateArr(splits(10),splits(11),splits(12),splits(13),splits(28),splits(29))(0)} " +
          s" ,${TimeUtil.promotionPriceDateArr(splits(10),splits(11),splits(12),splits(13),splits(28),splits(29))(1)},R10003"
        case _ => null
      }*/

    }).filter(x => {
      x != null
         //过滤开始日期和结束日期为null。
    }).filter(x => {
      val splits = x.split(",",-2)
      splits(4)!="0" && splits(5) !="0"        //过滤开始日期和结束日期为0。
    }).map(x => {
      //这里去除了关于分片路径的字符串信息。
      val splits = x.split(",",-2)
      Row(splits(0),splits(1), splits(2), splits(3), splits(4).toLong, splits(5).toLong, splits(6), splits(7))   //难道这里？？
    })
    //promotion_rdd.collect().foreach(println)
    val promotion_df = sqlSession.createDataFrame(promotion_rdd, promotionHiveTalSchema.promotionHiveTal)
    //promotion_df.collect(10).foreach(println)
    promotion_df

  }

  def startEndDate(splits:Array[String],one :Int ,two :Int ,three:Int ,four:Int)= {
    val arr= TimeUtil.dateTolongPromotion(TimeUtil.formateTransferPromotion(splits(one),splits(two)),TimeUtil.formateTransferPromotion(splits(three),splits(four)))
    arr
  }



}


