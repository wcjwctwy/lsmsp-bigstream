package cn.lsmsp.bigstream.rbiz.demo.example

import java.util.Date

import cn.lsmsp.sparkframe.realtime.Business
import org.apache.spark.rdd.RDD

class ExampleFileBusiness extends Business {
  override def business(rdd: RDD[(String, Any)]): Unit = {
    
    rdd
      .filter(x => Option(x._2).isDefined&&Option(x._2).get!="")
//      .foreach(println)
      .saveAsTextFile(s"/test/ExampleFileBusiness/${new Date().getTime.toString()}")
      
  }
}