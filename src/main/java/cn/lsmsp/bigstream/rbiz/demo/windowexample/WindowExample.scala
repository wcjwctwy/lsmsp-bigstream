package cn.lsmsp.bigstream.rbiz.demo.windowexample

import cn.lsmsp.sparkframe.common.util.RestUtils
import cn.lsmsp.sparkframe.realtime.Business
import cn.lsmsp.sparkframe.transform.MessageFactory
import cn.lsmsp.sparkframe.transform.message.{MessageListImpl, MessageObject, MessageObjectImpl}
import org.apache.spark.rdd.RDD

/**
  * Created by wangcongjun on 2017/6/1.
  */
class WindowExample extends Business {
  override def business(rdd: RDD[(String, Any)]): Unit = {
    if (!rdd.isEmpty()) {
      var mo: MessageObject = null
      val custCPU = rdd
//        .filter(x =>  Option(x._2).isDefined && Option(x._2).get != "")
        .filter(x =>  x._2!=null && x._2 != "")
        .filter(msg => MessageFactory.getMessageObjectFromJson(msg._2.toString).get("data/meta/brief").toString == "CPU利用率")
        .map(msg => {
          mo = MessageFactory.getMessageObjectFromJson(msg._2.toString)
          val data = getData(msg._2.toString)
          data.put("title","CPU单位时间阈值告警")
          val custid = mo.getString("data/assetId")
          val value = mo.getString("data/list/-1/values")
          (data.toString, value)
        })
        .groupByKey(10).map(x => {
        val sum = x._2.size
        var temp = 0
        x._2.foreach(str => {
          val v = str.toDouble
          if (v > 80) temp += 1
        })
        val flag = temp.toDouble / sum.toDouble * 100
        println(s"custid: ${x._1} ===temp: $temp sum: $sum flag: $flag")
        (x._1,flag)
      }).filter(_._2>80)
      if(!custCPU.isEmpty()){
        custCPU.foreach(msg=>RestUtils.post("http://10.20.1.32/bigdata/saveevent",s"data=${msg._1}"))
      }
      //      val sc = rdd.sparkContext
      //      val sql = new SQLContext(sc)
      //      val jsoRdd = rdd.flatMap(msg => {
      //        flat(msg._2)
      //      })
      //      sql.read.json(jsoRdd).registerTempTable("temptable")
      //      val sum = sql.sql(
      //        """
      //          |
      //          |select * from(
      //          |select t1.assetId,t1.entId,t1.ipAddress,t1.sum ,t2.temp ,(t2.temp / t1.sum) as value from
      //          |(select assetId,entId,ipAddress,count(assetId) as sum from temptable where  indexName = 'Cpu平均利用率' group by assetId,entId,ipAddress) t1 ,
      //          |(select assetId,entId,ipAddress,count(assetId) as temp from temptable where  indexName = 'Cpu平均利用率' and values > 80 group by assetId,entId,ipAddress) t2
      //          |where t1.assetId = t2.assetId and t1.entId = t2.entId
      //          |) t3 where t3.value > 0.8
      //        """.stripMargin).foreach(println)
    }
  }

  def flat(json: String): List[String] = {
    val obj = MessageFactory.getMessageObjectFromJson(json)
    val data = obj.pop("data").asInstanceOf[MessageObjectImpl]
    val meta = data.pop("meta").asInstanceOf[MessageObjectImpl]
    val list = data.pop("list").asInstanceOf[MessageListImpl].toList
    obj.addObject(data).addObject(meta)
    list.map(x => {
      val y = x.asInstanceOf[MessageObjectImpl]
      obj.addObject(y).toString
    })
  }
  def getData(json: String): MessageObjectImpl = {
    val obj = MessageFactory.getMessageObjectFromJson(json)
    val data = obj.pop("data").asInstanceOf[MessageObjectImpl]
    data.pop("meta").asInstanceOf[MessageObjectImpl]
    data.pop("list").asInstanceOf[MessageListImpl].toList
    data
  }

}
