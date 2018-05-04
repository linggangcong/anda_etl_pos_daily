package com.dr.schema

import org.apache.spark.sql.types._

/**
  * 流水数据的列类型定义
  */


object BannerBuySchema {    //https://blog.csdn.net/bitcarmanlee/article/details/52006718
  /**
    * 零售商数据模型类
    * bar_code            条形码
    * flow_no           流水号
    * banner_code         零售商号
    * pos_no              pos机号
    * retailer_shop_code 零售商店铺号
    * good_code           零售商商品号
    * trade_date_time      入机时间
    * trade_date_timestamp 入机时间戳
    * card_code           会员卡号
    * quantity           数量
    * price              价格
    * amount             总价
    * pay_type            支付类型
    * is_useful           流水是否有效
    * day                时间
    * banner             零售商
    */
  val bar_code = StructField("bar_code", StringType, true)   //常量  常量名指向一个类型的对象。  对象参数。
  val flow_no = StructField("flow_no", StringType, true)
  val banner_code = StructField("banner_code", StringType, true)
  val pos_no = StructField("pos_no", StringType, true)
  val retailer_shop_code = StructField("retailer_shop_code", StringType, true)
  val good_code = StructField("good_code", StringType, true)
  val trade_date_time = StructField("trade_date_time", StringType, true)
  val trade_date_timestamp = StructField("trade_date_timestamp", LongType, true)
  val card_code = StructField("card_code", StringType, true)
  val quantity = StructField("quantity", FloatType, true)
  val price = StructField("price", FloatType, true)
  val amount = StructField("amount", FloatType, true)
  //val pay_type = StructField("pay_type", StringType, true)
  //val is_useful = StructField("is_useful", IntegerType, true)
  val day = StructField("day", StringType, true)
  val banner = StructField("banner", StringType, true)

  val BannerBuy = StructType(Array(bar_code, flow_no, banner_code, pos_no, retailer_shop_code,
    good_code, trade_date_time, trade_date_timestamp, card_code, quantity, price, amount,
     day, banner))


}
