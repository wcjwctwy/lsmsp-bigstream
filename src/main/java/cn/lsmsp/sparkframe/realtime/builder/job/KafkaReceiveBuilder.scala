package cn.lsmsp.sparkframe.realtime.builder.job

import cn.lsmsp.sparkframe.realtime.adaptor.MsgAdaptor
import cn.lsmsp.sparkframe.realtime.entity.{AppContext, JobContext}
import cn.lsmsp.sparkframe.realtime.builder.Builder
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream

/**
  * 获取常规的数据输入流
  * Created by wangcongjun on 2017/6/26.
  */
class KafkaReceiveBuilder(
                           appContext: AppContext,
                           jobContext: JobContext) extends Builder {
  val source: String = jobContext.source
  val appName: String = appContext.appName
  val dataOptMethod: String = jobContext.method

  override def create(): DStream[(String, Any)] = {
    logInfo(s"app name: $appName read source: $source")
    val kafka = MsgAdaptor.kafkaReceive(appName, jobContext)._2
    val dStream = dataOptMethod match {
      case "window" => kafka.window(Milliseconds(jobContext.windowDuration), Milliseconds(jobContext.slideDuration))

      case _ => kafka
    }
    dStream.transform(rdd => {
      //解码消息
      rdd.mapPartitions(rp => {
        rp.map(msg => {
          //获取当前数据的所属topic
          val topic = jobContext.kafkaReceiveContext.topic
          val decoder = jobContext.decoders(topic)
          val content = decoder.fromBytes(msg._2)
          (topic, content)
        })
      })
    })
  }
}

object KafkaReceiveBuilder {
  def apply(
             appContext: AppContext,
             jobContext: JobContext): KafkaReceiveBuilder = new KafkaReceiveBuilder(appContext, jobContext)
}


