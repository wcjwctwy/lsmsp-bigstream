package cn.lsmsp.bigstream.rbiz.event

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, UUID}

import cn.lsmsp.sparkframe.common.util.FileUtils
import cn.lsmsp.sparkframe.realtime.Business
import org.apache.solr.client.solrj.beans.Field
import org.apache.solr.client.solrj.impl.CloudSolrServer
import org.apache.spark.streaming.dstream.DStream

import scala.annotation.meta.field

class EventClassity2Solr extends Business {
  val DEFAUTVALUE = "404"
  val hConf = FileUtils.readJsonFile2Prop("serverconfig", "hbase")

  val transTime: Long => String = num => {
    val date = new Date(num * 60000)
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    format.format(date)
  }
  override def business(ds: DStream[(String, Any)]): Unit = {
    val eventDs = ds.foreachRDD(rdd => {
      //      rdd.foreach(println)
      //      rdd.foreach(rdd=>println(rdd._2.toString.split("\t").length))
      rdd.map(msg => {
        val msgs = msg._2.asInstanceOf[Map[String, Any]]
        ((msgs.getOrElse("entid", DEFAUTVALUE),
          msgs.getOrElse("cusid", DEFAUTVALUE),
          msgs.getOrElse("eventcategory", DEFAUTVALUE),
          msgs.getOrElse("eventcategorytechnique", DEFAUTVALUE),
          msgs.getOrElse("eventlevel", DEFAUTVALUE),
          msgs.getOrElse("eventname", DEFAUTVALUE),
          msgs.getOrElse("categorydevice", DEFAUTVALUE),
          msgs.getOrElse("deviceaddress", DEFAUTVALUE),
          msgs.getOrElse("devicereceipttime", DEFAUTVALUE).toString.toLong / 60000,
          msgs.getOrElse("isanalyzerevent", DEFAUTVALUE).toString.toInt), 1.toLong)
      })
        .reduceByKey(_ + _)
        .foreachPartition(it=>{
          val hss = new CloudSolrServer("10.20.4.160:2181,10.20.4.161:2181,10.20.4.162:2181/solr")
          hss.setDefaultCollection("spark_test")
          val docs: util.Collection[EventCount] = new util.ArrayList[EventCount]
          while(it.hasNext){
            val t = it.next()
           val ec = EventCount(
              t._1._1.toString.toLong,
              t._1._2.toString.toLong,
              t._1._3.toString,
              t._1._4.toString,
              t._1._5.toString,
              t._1._6.toString,
              t._1._7.toString,
              t._1._8.toString,
              transTime(t._1._9),
              t._1._10,
              t._2.toString.toLong,
              UUID.randomUUID().toString
            )
            docs.add(ec)
          }
          if(docs.size()>0){
            hss.addBeans(docs)
            hss.optimize()
            hss.commit()
          }
        })
    })
  }
}
case class EventCount(
                       @(Field@field)("entid") ent:Long,
                       @(Field@field)cusid:Long,
                       @(Field@field)eventcategory:String,
                       @(Field@field)eventcategorytechnique:String,
                       @(Field@field)eventlevel:String,
                       @(Field@field)eventname:String,
                       @(Field@field)categorydevice:String,
                       @(Field@field)deviceaddress:String,
                       @(Field@field)devicereceipttime:String,
                       @(Field@field)isanalyzerevent:Int,
                       @(Field@field)eventcount:Long,
                       @(Field@field)id:String
                     ){}
