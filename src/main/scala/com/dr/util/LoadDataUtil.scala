package com.dr.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.log4j.Logger
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.SparkContext
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.sql.SparkSession
/**
  * 以不同的文件编码加载数据filesplitsparkcon
  */
object LoadDataUtil {
  val logger = Logger.getLogger(LoadDataUtil.getClass)
  /**
    * 指定编码打开文件，返回文件名和编码格式
    *
    * @param sc
    * @param filepath
    * @param encoding
    * @return
    */

  //功能：读入Hadoop文件，转化为hadoopRDD(RDD子类),返回一个RDD[String ,String].
  def loadFileToRdd(sc: SparkContext, filepath: String, encoding: String = "UTF-8" ,endDate:String) = {    //filepath:  hdfs://192.168.0.151:9000/user/root/input/anda_pos/201[5678]*/金额*
    /*val conf = new Configuration()
                                                //
    val filePath = new Path(filepath)
    val fileSystem = filePath.getFileSystem(conf)
    if (!fileSystem.exists(filePath)) {
      logger.error("数据文件路径不存在！")
      AndaEtlLogUtil.produceEtlMyjErrorLog(endDate, "数据文件路径不正确，程序退出")   //endDate，表示程序执行日的前一天，也是最后执行的数据日期日
      System.exit(1)
    }*/
    val fileRdd = sc.hadoopFile[LongWritable, Text, TextInputFormat](filepath)    //从Hadoop读取 testfile 文件。key是行偏移量，value是一行的字符串 ;返回RDD。
    val hadoopRdd = fileRdd.asInstanceOf[HadoopRDD[LongWritable, Text]]    //将引用转换为子类的引用。
    val fileNameAndLine = hadoopRdd.mapPartitionsWithInputSplit((inputSplit:InputSplit,iterator:Iterator[(LongWritable,Text)]) => {    //匿名函数的语法很简单，箭头左边是参数列表，右边是函数体
      val fileSplit = inputSplit.asInstanceOf[FileSplit]        //file 是FileSplit引用 ，一个分片。
      iterator.filter(x => x._2 != null&&x._2.getLength !=0 ).map( x => {      //匿名函数， x输入参数。 函数体x._2 != null  //去除了文件的空行。
        //System.out.println("分片数据路径名： "+ x._1 )
        (fileSplit.getPath.getName, new String(x._2.getBytes,0, x._2.getLength,encoding) )    //x._2  元组的第2个元素。 根据分片获取文件名， text默认转码到UTF-8。
      })
      //(fileSplit.getPath.getName, new String(x._2.getBytes,0, x._2.getLength,encoding) )
    })   //在rdd内部对每个元素应用函数。
    //fileNameAndLine.collect().foreach(println)
    //logger.info("读取数据文件：" + filepath)     //
    System.out.println("读取数据文件： "+ filepath )
    fileNameAndLine
  }
}
