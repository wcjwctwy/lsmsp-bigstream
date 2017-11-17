package cn.lsmsp.bigstream.rbiz.event

import cn.lsmsp.sparkframe.realtime.Business
import cn.lsmsp.sparkframe.transform.decode.EventMeta
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SaveMode}

/**
  * Created by wangcongjun on 2017/6/14.
  */
class EventClassifyBusiness extends Business{

  override def business(rdd: RDD[(String, Any)]): Unit = {
    val sql = new SQLContext(rdd.sparkContext)
    if (!rdd.isEmpty()) {
//            rdd.foreach(x=>println(x._2.getClass))
      val classfyRDD = rdd.filter(_._1 == "lsmsp-E2C-Gather-event")
        .map(msg=>{
          val map = msg._2.asInstanceOf[Map[String,Any]]
          /**
            * 以schema为主 把数据按照schema的顺序和类型进行编排
            */
          EventMeta.schema.map(s=>map(s.name))
        })

        .map(Row(_: _*))
      val frame = sql.createDataFrame(classfyRDD, EventMeta.schema)
        .registerTempTable("a_streaming_event")
//        sql.sql("select * from a_streaming_event")
      sql.sql(
        """
          |select entid as entId,cusid as assetid,eventcategory as eventCategory,eventcategorytechnique as EventCategoryTechnique,eventlevel as EventLevel,eventname as EventName,categorydevice as CategoryDevice,deviceaddress as DeviceAddress,floor(devicereceipttime/60000) as DeviceReceiptTime ,isanalyzerevent as IsAnalyzerEvent ,sum(eventcount) as sum  from a_streaming_event
          |group by entId,cusid,eventcategory,eventcategorytechnique,eventlevel,eventname,categorydevice,deviceaddress,floor(devicereceipttime/60000) ,isanalyzerevent
  """.stripMargin)
//        .show()

       .coalesce(1).write
              .mode(SaveMode.Append)
              .parquet("/user/hive/warehouse/app.db/a_streaming_eventclass1")
    }
  }
}
