package cn.lsmsp.bigstream.rbiz.event

import java.text.SimpleDateFormat
import java.util.Date

import cn.lsmsp.sparkframe.realtime.Business
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by wangcongjun on 2017/6/14.
  */
class EventClassifyNoSqlBusiness extends Business {
  val DEFAUTVALUE = "404"
  val transTime:Long=>String =num=>{
    val date = new Date(num*60000)
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    format.format(date)
  }
  override def business(rdd: RDD[(String, Any)]): Unit = {
    val sql = new SQLContext(rdd.sparkContext)
    if (!rdd.isEmpty()) {
      //      rdd.foreach(println)
      //      rdd.foreach(rdd=>println(rdd._2.toString.split("\t").length))
      val classfyRDD = rdd
        .map(msg => {
          val msgs = msg._2.asInstanceOf[Map[String,Any]]
          ((msgs.getOrElse("entid",DEFAUTVALUE),
            msgs.getOrElse("cusid",DEFAUTVALUE),
            msgs.getOrElse("eventcategory",DEFAUTVALUE),
            msgs.getOrElse("eventcategorytechnique",DEFAUTVALUE),
            msgs.getOrElse("eventlevel",DEFAUTVALUE),
            msgs.getOrElse("eventname",DEFAUTVALUE),
            msgs.getOrElse("categorydevice",DEFAUTVALUE),
            msgs.getOrElse("deviceaddress",DEFAUTVALUE),
            //            msgs(dic.getOrElse("devicereceipttime",DEFAUTINDEX)),
            msgs.getOrElse("devicereceipttime",DEFAUTVALUE).toString.toLong / 60000,
            msgs.getOrElse("isanalyzerevent",DEFAUTVALUE).toString.toInt), 1.toLong)
        })
        .reduceByKey(_ + _)
        .map { x =>

          Row(x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._1._6, x._1._7, x._1._8, x._1._9, x._1._10,transTime(x._1._9), x._2)
        }
      val schemaString = "entId,assetid,eventCategory,EventCategoryTechnique,EventLevel,EventName,CategoryDevice,DeviceAddress,DeviceReceiptTime,IsAnalyzerEvent,ReceiptTime,count"
      val schema =
        StructType(
          schemaString.split(",").map {
            case fieldName: String if fieldName == "count" => StructField(fieldName, LongType)
            case fieldName: String if fieldName == "DeviceReceiptTime" => StructField(fieldName, LongType)
            case fieldName: String if fieldName == "IsAnalyzerEvent" => StructField(fieldName, IntegerType)
            case fieldName: Any => StructField(fieldName, StringType)
          })

      val frame = sql.createDataFrame(classfyRDD, schema)
//                                .show()
        .coalesce(2).write
        .mode(SaveMode.Append)
      frame.parquet("/user/hive/warehouse/app.db/a_streaming_eventclass_src/")
    }
  }
}