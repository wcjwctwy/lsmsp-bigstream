package cn.lsmsp.sparkframe.realtime

import cn.lsmsp.sparkframe.common.log.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

trait Business extends Serializable with Logging{
  def business(rdd: RDD[(String, Any)]): Unit={}
  def business(dStream: DStream[(String, Any)]): Unit={}
  def start():Unit={}
}