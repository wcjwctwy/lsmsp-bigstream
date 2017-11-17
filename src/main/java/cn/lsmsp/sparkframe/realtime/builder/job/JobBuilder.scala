package cn.lsmsp.sparkframe.realtime.builder.job

import cn.lsmsp.sparkframe.realtime.adaptor.MsgAdaptor
import cn.lsmsp.sparkframe.realtime.entity.{AppContext, JobContext}
import cn.lsmsp.sparkframe.realtime.builder.Builder
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.SparkFlumeEvent

/**
  * job生成器；根据appName和job的上下文来生成对的job实例
  * 主要是获取数据输入流
  * Created by wangcongjun on 2017/6/26.
  */
object JobBuilder extends Builder {

  override def create(appContext: AppContext,jobContext: JobContext): DStream[(String, Any)] = {
    val source = jobContext.source
    logInfo(s"read source: $source")
    source match {
      case "kafkaDirect" => KafkaDirectBuilder(appContext, jobContext).create()
      case "kafkaReceive" => KafkaReceiveBuilder(appContext, jobContext).create()
      case "flumePoll"=> FlumePollBuilder.create(appContext,jobContext)

      case _ => throw new Exception(s"数据源名称不对：$source")
    }
  }

}

//object JobBuilder {
//  def apply(
//             appContext: AppContext
//
//           ): JobBuilder = new JobBuilder(appContext)
//}
