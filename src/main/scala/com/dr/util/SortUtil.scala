package com.dr.util

/**
  * Created by SAM on 2018/5/11.
  */
import java.util.Comparator

class SortUtil extends Comparator[String] {

  override def compare( arg0 :String ,  arg1 :String): Int= {

    val date0 =  arg0.asInstanceOf[String]
    val date1 =  arg1.asInstanceOf[String]
   date0.compareTo(date1)

  }

}
