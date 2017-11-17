package cn.lsmsp.bigstream.dbiz.event

import cn.lsmsp.sparkframe.realtime.adaptor.SparkContextAdaptor
import org.apache.spark.sql.{SQLContext, SaveMode}

/**
  * Created by wangcongjun on 2017/7/25.
  */
object EventCountMerge {
  val appName = "EventCountMerge"

  def main(args: Array[String]): Unit = {
    val sc = SparkContextAdaptor.getSC(appName)
    val sqlContext = new SQLContext(sc)
    //    val fileName = DateUtils.getCurrentFormatDate(DateUtils.pattern3)
    val frame = sqlContext.read.parquet("/user/hive/warehouse/app.db/a_streaming_eventclass_tmp/*.parquet")
    if(!frame.rdd.isEmpty()){
      frame
        .coalesce(1).write.mode(SaveMode.Append).parquet("/user/hive/warehouse/app.db/a_streaming_eventclass")
    }
    sc.stop()
  }
}
