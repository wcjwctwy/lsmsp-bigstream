package cn.lsmsp.bigstream.rbiz.demo.sqlanalysis

import cn.lsmsp.sparkframe.realtime.Business
import cn.lsmsp.sparkframe.transform.MessageFactory
import cn.lsmsp.sparkframe.transform.message.{MessageListImpl, MessageObjectImpl}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by wangcongjun on 2017/6/6.
  */
class SqlAnalysis extends Business{
  override def business(rdd: RDD[(String, Any)]): Unit = {
    val sc = rdd.sparkContext
    val sql = new SQLContext(sc)
    val jsoRdd = rdd.flatMap(msg=>{
      flat(msg._2.toString)
    })
    sql.read.json(jsoRdd).registerTempTable("temptable")
    sql.sql("select count(*) as sum from temptable")
      .show()
//      .write.parquet("")
  }

  def flat(json:String): List[String] ={
    val obj = MessageFactory.getMessageObjectFromJson(json)
    val data = obj.pop("data").asInstanceOf[MessageObjectImpl]
    val meta = data.pop("meta").asInstanceOf[MessageObjectImpl]
    val list = data.pop("list").asInstanceOf[MessageListImpl].toList
    obj.addObject(data).addObject(meta)
    list.map(x=>{
      val y = x.asInstanceOf[MessageObjectImpl]
      obj.addObject(y).toString
    })
  }
}
