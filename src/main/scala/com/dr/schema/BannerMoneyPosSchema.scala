package com.dr.schema

import org.apache.spark.sql.types._

object BannerMoneyPosSchema {    //https://blog.csdn.net/bitcarmanlee/article/details/52006718

  val pay_type = StructField("pay_type", StringType, true)   //常量  常量名指向一个类型的对象。  对象参数。
  val flow_no = StructField("flow_no", StringType, true)
  val is_useful = StructField("is_useful", StringType, true)
  val BannerMoneyPos = StructType(Array(flow_no, pay_type, is_useful))

}

