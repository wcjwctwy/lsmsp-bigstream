package cn.lsmsp.sparkframe.realtime.builder.job

import cn.lsmsp.sparkframe.realtime.adaptor.MsgAdaptor
import cn.lsmsp.sparkframe.realtime.builder.Builder
import cn.lsmsp.sparkframe.realtime.entity.{AppContext, JobContext}
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.SparkFlumeEvent

/**
  * 获取常规的数据输入流
  * Created by wangcongjun on 2017/6/26.
  */
object FlumePollBuilder extends Builder {
  val flumDS=scala.collection.mutable.Map[String,DStream[(String,SparkFlumeEvent)]]()
  override def create(
                       appContext: AppContext,
                       jobContext: JobContext
                     ): DStream[(String, Any)] = {
    val source: String = jobContext.source
    val appName: String = appContext.appName
    val dataOptMethod: String = jobContext.method
    logInfo(s"app name: $appName read source: $source method: $dataOptMethod")
    val flumeUrl = jobContext.brokers
    if(flumDS.get(flumeUrl).isEmpty) {
      val flume = MsgAdaptor.flumePoll(appContext.appName, jobContext)._2
      flumDS+=flumeUrl->flume
    }
   val  flume = flumDS(flumeUrl)
    val dStream = dataOptMethod match {
      case "window" => flume.window(Milliseconds(jobContext.windowDuration), Milliseconds(jobContext.slideDuration))

      case _ => flume
    }
    dStream.transform(rdd => {
      //解码消息
      rdd.mapPartitions(rp => {
        rp.map(msg => {
          //获取当前数据的所属topic
//          val topic = jobContext.flumePollContext.topic
          val decoder = jobContext.decoders(msg._1)
          val content = decoder.fromBytes(msg._2.event.getBody.array())
          (msg._1, content)
        })
      })
    })
  }
}





