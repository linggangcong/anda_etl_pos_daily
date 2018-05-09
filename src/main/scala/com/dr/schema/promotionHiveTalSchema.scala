package com.dr.schema

import org.apache.spark.sql.types._

/**
  * 流水数据的列类型定义
  */


object promotionHiveTalSchema {    //https://blog.csdn.net/bitcarmanlee/article/details/52006718
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
  val promotion_code = StructField("promotion_code", StringType, true)   //常量  常量名指向一个类型的对象。  对象参数。
  val promotion_good_id = StructField("promotion_good_id", StringType, true)
  val promotion_shop_id = StructField("promotion_shop_id", StringType, true)
  //val pos_no = StructField("pos_no", StringType, true)
  val promotion_type = StructField("promotion_type", StringType, true)
  val promotion_start= StructField("promotion_start", LongType, true)
  val promotion_end= StructField("promotion_end", LongType, true)
  val promotion_banner= StructField("promotion_banner", StringType, true)
  val promotion_banner_code = StructField("promotion_banner_code", StringType, true)


  val promotionHiveTal = StructType(Array(promotion_code, promotion_good_id, promotion_shop_id, promotion_type,
    promotion_start, promotion_end, promotion_banner,promotion_banner_code))


}
