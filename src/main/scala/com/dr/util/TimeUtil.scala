package com.dr.util

import java.io.FileNotFoundException
import java.text.{DecimalFormat, ParseException, SimpleDateFormat}
import java.lang.Double
import java.util.{Calendar, Date}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.internal.util.Collections


/**
  * 处理时间的方法
  */
object TimeUtil {
  //促销内，整理日期格式。yyyy-MM-dd HH:mm:ss
  def formateTransferPromotion( first :String  , second :String):String={
    var dateTimeStr=""
    var	mm=""
    var ss="00"
    var hh=""
    if(second.length()<4){
      mm =second+"0"
      hh="00"
    }else{
      hh=second.substring(0, 2)
      mm=second.substring(2, 4)
    }
    var firstArr = first.split(" ",-2)
    dateTimeStr=firstArr(0).replace("/", "-")+" "+hh+":"+mm+":"+ss
    //dateTimeStr=first.substring(0, 10).replace("/", "-")+" "+hh+":"+mm+":"+ss;
    dateTimeStr
  }

  //传入两个字符串，获取两个日期的毫秒值。
  def dateTolongPromotion(date1:String, date2:String ): Array[Long] ={
    var timeInMillis = new Array[Long](2)
    var calendar = Calendar.getInstance()
    try {
      calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date1))
    } catch {
      case ex: ParseException => {
        println("data parsing exception")
      }
    }
    timeInMillis(0)= calendar.getTimeInMillis()
    try {
      calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(date2))
    } catch {
      case ex: ParseException => {
        println("data parsing exception")
      }
    }
    timeInMillis(1)= calendar.getTimeInMillis()
    timeInMillis
  }


  //返回开始时间和结束时间的毫秒数据。传入表格中多个日期字符串，根据表格逻辑判断处理。 用于进价售价
  def promotionPriceDateArr( first :String  , second :String, third :String  , forth :String, fifth :String  , sixth :String): Array[Long]= {
    var startDateTime=""
    var endDateTime=""
    var dateLongArr= new Array[Long](2)
    if(second.length()<5){    //就是进价优惠和售价优惠起止时间不同。
      if(fifth.length()<15 ||sixth.length()<15 ){
        dateLongArr(0)=0
        dateLongArr(1)=0

      }else{
        startDateTime=this.formateTransferPromotion(fifth,third)
        endDateTime=this.formateTransferPromotion(sixth,forth)
      }

    }else{
      if(first.length()<11 ||second.length()<11 ){
        dateLongArr(0) =0
        dateLongArr(1) =0

      } else{
        startDateTime=this.formateTransferPromotion(first,third)
        endDateTime=this.formateTransferPromotion(second,forth)
      }

    }
     this.dateTolongPromotion(startDateTime, endDateTime)

  }


  /**
    * 把入机时间格式处理，并且变成毫秒单位。
    *
    * @param strDate
    */
  def dateShaped(strDate: String): String = {
    var strDate1 = strDate.replace("/", "-")
    if(strDate1.length()>19){
      strDate1= strDate.substring(0, strDate1.lastIndexOf(":"))
      val splitArray =strDate1.split(" ",-2)
      if(splitArray(0).length <10 ){
        null
      }else{
        strDate1
      }
    }else{
      null
    }
  }


  def dataToLong(strDate: String): Long = {
    if(strDate==null){
      0
  }else {
      var timeInMillis = 0
      var calendar = Calendar.getInstance()
      try {
        calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(strDate))
      } catch {
        case ex: Exception => {
          println("data parsing exception")
        }
      }
      calendar.getTimeInMillis()
    }
  }



  // 输入数量和总额字符串数据，得到格式化的单价数据。
  def priceStr(numStr: String ,amountStr:String): String = {
    val df = new DecimalFormat("#.00")
    var priceStr=""
    var priceFormated=0.0d
    var price= 0.0d

    if(numStr.isEmpty() || numStr==null || numStr=="0"){
      priceStr=null

    }else{
      price =  Double.valueOf(amountStr)/ Double.valueOf(numStr)
      priceStr=df.format(price)
      try{
        priceFormated=Double.valueOf(priceStr)
      }catch{
        case e:Exception =>
          println(e)
      }
      priceStr= Double.toString(priceFormated)

    }
    priceStr
  }

  /**
    * 获得系统当前日期
    *
    * @return
    */
  def getNowDateTimeStamp(): String = {
    val now: Date = new Date()
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val nowStr = dateFormat.format(now)
    nowStr
  }

/*

  /**
    * 把字符串格式的日期转化为时间戳
    *
    * @param strDate
    */
  def parseStrDateToTimeStamp(strDate: String) = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    try {
      if (strDate == null || strDate.trim.equals("")) 0L
      else {
        val d = format.parse(strDate)
        d.getTime
      }
    } catch {
      case e: Exception => 0L
    }
  }

  /**
    * 截取时间，去掉毫秒级别
    *
    * @param tradeDateTime 2016-01-04 00:20:27.143
    * @return
    */
  def subDateTime(tradeDateTime: String) = {

    if (tradeDateTime == null || tradeDateTime.trim.equals("")) {
      null
    } else {
      val date_regex = "(\\d{4}-\\d{2}-\\d{2}\\s+\\d{2}:\\d{2}:\\d{2})\\.*\\d*".r
      try {
        val date_regex(date) = tradeDateTime
        date
      } catch {
        case e: Exception => {
          null
        }
      }
    }

  }*/

  def getDateListStartAndEnd(start_date: String, end_date: String) = {   //输入20180423  20180423  返回路径列表Array(String)。

    try {
      val dateFormat = new SimpleDateFormat("yyyyMMdd")
      val dateFiled = Calendar.DAY_OF_MONTH
      var beginDate = dateFormat.parse(start_date)
      val endDate = dateFormat.parse(end_date)
      val calendar = Calendar.getInstance()
      calendar.setTime(beginDate)
      val dateArray: ArrayBuffer[String] = ArrayBuffer()
      while (beginDate.compareTo(endDate) <= 0) {
        dateArray += dateFormat.format(beginDate)
        calendar.add(dateFiled, 1)
        beginDate = calendar.getTime
      }
      dateArray.toList
    } catch {
      case e: Exception => null
    }
  }

  def getLatestDay(dateList: List[String]): String = {
    //val sort = new SortUtil()
   // Collections
    if (dateList == null || dateList.isEmpty) return null
    dateList.sortWith((s,t) => s.compareTo(t) <0 )
    //dateList(dateList.size - 1)
    dateList.last
  }
}
