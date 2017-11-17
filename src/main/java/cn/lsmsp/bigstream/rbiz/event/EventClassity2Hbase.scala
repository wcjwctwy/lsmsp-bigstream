package cn.lsmsp.bigstream.rbiz.event

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

import cn.lsmsp.sparkframe.common.util.FileUtils
import cn.lsmsp.sparkframe.realtime.Business
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.DStream

class EventClassity2Hbase extends Business {
  val DEFAUTVALUE = "404"
  val hConf = FileUtils.readJsonFile2Prop("serverconfig", "hbase")

  val transTime: Long => String = num => {
    val date = new Date(num * 60000)
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    format.format(date)
  }

  override def business(ds: DStream[(String, Any)]): Unit = {
    val eventDs = ds.transform(rdd => {
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
        .map { x =>
          val put = new Put(Bytes.toBytes(UUID.randomUUID().toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entid"), Bytes.toBytes(x._1._1.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("cusid"), Bytes.toBytes(x._1._2.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("eventcategory"), Bytes.toBytes(x._1._3.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("eventcategorytechnique"), Bytes.toBytes(x._1._4.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("eventlevel"), Bytes.toBytes(x._1._5.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("eventname"), Bytes.toBytes(x._1._6.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("categorydevice"), Bytes.toBytes(x._1._7.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("deviceaddress"), Bytes.toBytes(x._1._8.toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("devicereceipttime"), Bytes.toBytes(transTime(x._1._9).toString))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("isanalyzerevent"), Bytes.toBytes(x._1._10))
          put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("eventcount"), Bytes.toBytes(x._2))
          put

        }
    })
    val tableName = TableName.valueOf(hConf.getProperty("eventClassTable"))
    val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set(HConstants.ZOOKEEPER_QUORUM, hConf.getProperty("zkQuorum"))
    val hbaseContext = new HBaseContext(eventDs.context.sparkContext, hbaseConfig)
    hbaseContext.streamBulkPut[Put](eventDs, tableName, (put) => put)
  }
}
