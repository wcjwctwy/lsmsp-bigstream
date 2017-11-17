package cn.lsmsp.bigstream.rbiz.demo.example

import cn.lsmsp.sparkframe.realtime.Business
import org.apache.spark.rdd.RDD

class ExampleBusiness extends Business {
  override def business(rdd: RDD[(String, Any)]): Unit = {
    rdd
      .filter(x => Option(x._2).isDefined&&Option(x._2).get!="")
            .foreach(println)
  }
}