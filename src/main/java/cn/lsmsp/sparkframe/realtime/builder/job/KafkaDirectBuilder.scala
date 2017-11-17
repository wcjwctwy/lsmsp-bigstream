package cn.lsmsp.sparkframe.realtime.builder.job

import cn.lsmsp.sparkframe.realtime.adaptor.MsgAdaptor
import cn.lsmsp.sparkframe.realtime.entity.{AppContext, JobContext}
import cn.lsmsp.sparkframe.realtime.builder.Builder
import cn.lsmsp.sparkframe.realtime.offset.OffsetOptImpl
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, OffsetRange}

/**
  * 获取常规的数据输入流
  * Created by wangcongjun on 2017/6/26.
  */
class KafkaDirectBuilder(
                          appContext: AppContext,
                          jobContext: JobContext) extends Builder {
  var offsetRanges: Array[OffsetRange] = _
  val dataOptMethod: String = jobContext.method
  val appName: String = appContext.appName

  override def create(): DStream[(String, Any)] = {
    logInfo(s"app name: $appName read source: $dataOptMethod")
    val kafka = MsgAdaptor.kafkaDirect(appName, jobContext)
      ._2.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })
    val dStream = dataOptMethod match {
      case "window" => kafka.window(Milliseconds(jobContext.windowDuration), Milliseconds(jobContext.slideDuration))

      case _ => kafka
    }

    dStream.transform(rdd => {
      //存储offset
      logInfo("tasks success !!save offset!!" + offsetRanges)
      val kafkaDirectContext = jobContext.kafkaDirectContext
      val offsetOptImpl = OffsetOptImpl(kafkaDirectContext.zookeeper)
      offsetOptImpl.saveOffset(offsetRanges, kafkaDirectContext.groupId)
      //解码消息  可以认为也是一个数据处理  其实更像一个adaptor 使得输入的数据能够和现有的处理规则匹配到一起
      rdd.mapPartitions(rp => {
        rp.map(msg => {
          val topic = msg._1
          val decoder = jobContext.decoders(topic)
          val content = decoder.fromBytes(msg._2).toString
          (topic, content)
        })
      })

    })
  }
}

object KafkaDirectBuilder {
  def apply(
             appContext: AppContext,
             jobContext: JobContext): KafkaDirectBuilder = new KafkaDirectBuilder(appContext, jobContext)
}


