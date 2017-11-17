package cn.lsmsp.sparkframe.realtime.adaptor

import java.net.InetSocketAddress

import cn.lsmsp.sparkframe.realtime.entity.JobContext
import cn.lsmsp.sparkframe.realtime.offset.OffsetOptImpl
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
  * Created by wangcongjun on 2017/4/17.
  * 消息转换接口用来转换消息
  */
object MsgAdaptor extends Logging {
  def kafkaDirect(appName: String, jobContext: JobContext): (StreamingContext, DStream[(String, Array[Byte])]) = {
    val kafkaDirectContext = jobContext.kafkaDirectContext
    val ssc = SparkContextAdaptor.getSSC(appName)
    //kafka的配置参数
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> jobContext.brokers)
    //topic partition offset 对应值//校验offset值
    val fromOffsets: Map[TopicAndPartition, Long] = OffsetOptImpl(kafkaDirectContext.zookeeper).offsetPriority(kafkaDirectContext.groupId, jobContext.kafkaDirectContext.tpos, jobContext.brokers)
    logInfo(s"fromOffset  =  $fromOffsets")
    val messageHandler = (mam: MessageAndMetadata[String, Array[Byte]]) => (mam.topic, mam.message())
    val InputDS = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder, (String, scala.Array[scala.Byte])](ssc, kafkaParams, fromOffsets, messageHandler)
    (ssc, InputDS)
  }

  def kafkaReceive(appName: String, jobContext: JobContext): (StreamingContext, DStream[(String, Array[Byte])]) = {
    val kafkaReceiveContext = jobContext.kafkaReceiveContext
    val ssc = SparkContextAdaptor.getSSC(appName)
      //kafka的配置参数
      val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> jobContext.brokers,
        "group.id" -> kafkaReceiveContext.groupId,
        "zookeeper.connect" -> kafkaReceiveContext.zookeeper,
        "auto.offset.reset" -> kafkaReceiveContext.offsetReset)
      //失败从什么地方开始读取offset  smallest  从头开始  largest 从尾开始
      val topics: Map[String, Int] = Map[String, Int](kafkaReceiveContext.topic -> kafkaReceiveContext.numThreads)
      val InputDS = KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, topics, StorageLevel.MEMORY_AND_DISK).transform(_.partitionBy(new RollPartition(10)))
      (ssc, InputDS)
  }

  //poll flume
  def flumePoll(appName: String, jobContext: JobContext): (StreamingContext, DStream[(String,SparkFlumeEvent)]) = {
    val jobName = jobContext.jobName
    val brokers = jobContext.brokers
    val topic = jobContext.flumePollContext.topic
    if (brokers == null || brokers == "") new Exception(s"realtime.json中$appName 作业名为$jobName 的brokers没有配置！！ ")
    val ssc = SparkContextAdaptor.getSSC(appName)
    val addresses = jobContext.brokers.split(",").map(broker => {
      val s = broker.split(":")
      new InetSocketAddress(s(0), s(1).toInt)
    }).toSeq
    val flume = FlumeUtils.createPollingStream(ssc, addresses, StorageLevel.MEMORY_AND_DISK).transform(_.map(s=>{
      (topic,s)
    }).partitionBy(new RollPartition(10)))
    (ssc, flume)
  }
}
