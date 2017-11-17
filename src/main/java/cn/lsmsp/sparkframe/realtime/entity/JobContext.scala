package cn.lsmsp.sparkframe.realtime.entity

import cn.lsmsp.sparkframe.realtime.InitContext
import cn.lsmsp.sparkframe.transform.DataUtils
import cn.lsmsp.sparkframe.transform.message.{MessageList, MessageObjectImpl}
import kafka.common.TopicAndPartition
import kafka.serializer.Decoder

/**
  * Created by wangcongjun on 2017/6/29.
  */
private[entity]
case class KafkaDirectContext(zookeeper:String,groupId:String,tpos:Map[TopicAndPartition,Long]){}
private[entity]
case class KafkaReceiveContext(zookeeper:String,groupId:String,numThreads:Int,offsetReset:String,topic:String){}
private[entity]
case class FlumePollContext(numThreads:Int,eventNum:Int,topic:String){}

class JobContext(job:MessageObjectImpl, _task:List[TaskContext], envContext: EnvContext) extends Context {
  private val zookeeper:String = getOrElse(InitContext.ZK,envContext.zookeeper)
  val brokers:String = getOrElse(InitContext.BROKERS,envContext.brokers)
  val jobName:String = job.getString(InitContext.JOBNAME)
  private val groupId:String = job.getString(InitContext.GROUPID)
  val windowDuration:Int = job.getInt(InitContext.WIDDOWDURATION)
  //1-不执行 2-正常执行
  val status:Int = job.getInt(InitContext.STATUS)
  val slideDuration:Int = job.getInt(InitContext.SLIDEDURATION)
  val checkPath:String  = job.getString(InitContext.CHECKPATH)
  val source:String = job.getString(InitContext.SOURCE)
  val method:String = job.getString(InitContext.METHOD)
  private val numThreads:Int = job.getInt(InitContext.NUMTHREADS)
  private val offsetReset:String = job.getString(InitContext.OFFSETRESET)
  private val eventNum:Int = job.getInt(InitContext.EVENTNUM)
  val task:List[TaskContext] = _task
  //topic partition offset
  private val tpos:Map[TopicAndPartition,Long] = getTopicAndPartitionAndOffsets(job.getList(InitContext.TOPICLIST))

  //topic 对应的解码器
  val decoders : Map[String,Decoder[Any]] =getTopicsDecoder(job.getList(InitContext.TOPICLIST))


  //构建kafkaDirectContext
  def kafkaDirectContext :KafkaDirectContext= {
    //校验传入的参数
    if (brokers == null || brokers == "") throw  new Exception(s"realtime.json中 作业名为$jobName 的brokers没有配置！！ ")
    if(groupId==null||groupId=="")throw  new Exception(s"realtime.json中 作业名为$jobName 的groupId没有配置！！ ")
    if(zookeeper==null||zookeeper=="")throw  new Exception(s"realtime.json中 作业名为$jobName 的zookeeper没有配置！！ ")
    KafkaDirectContext(zookeeper,groupId,tpos)
  }


//构建KafkaReceiveContext
  def kafkaReceiveContext :KafkaReceiveContext= {
    //校验传入的参数
    if (brokers == null || brokers == "") throw  new Exception(s"realtime.json中 作业名为$jobName 的brokers没有配置！！ ")
    val t = tpos.map(_._1.topic).toSet
    if (t.size > 1) throw new Exception(s"realtime.json中作业名为$jobName 的topic有且只能有一个 多个可以再启动一个job作业！！！ ")
    if (groupId == null || groupId == "") throw new Exception(s"realtime.json中 作业名为$jobName 的groupId没有配置！！ ")
    if (brokers == null || brokers == "") throw new Exception(s"realtime.json中作业名为$jobName 的brokers没有配置！！ ")
    if (zookeeper == null || zookeeper == "") throw new Exception(s"realtime.json中作业名为$jobName 的zookeeper没有配置！！ ")
    if (offsetReset == null || offsetReset == "")throw new Exception(s"realtime.json中作业名为$jobName 的offsetReset没有配置(largest or smallest)！！ ")
    if (numThreads == 0)throw new Exception(s"realtime.json中作业名为$jobName 的numThreads没有配置！！ ")
    KafkaReceiveContext(zookeeper,groupId,numThreads,offsetReset,t.head)
  }


  //构建FlumePollContext
  def flumePollContext:FlumePollContext={
    //校验传入的参数
    val t = tpos.map(_._1.topic).toSet
    if (t.size > 1) throw new Exception(s"realtime.json中作业名为$jobName 的topic有且只能有一个 多个可以再启动一个job作业！！！ ")
    if (brokers == null || brokers == "") throw  new Exception(s"realtime.json中 作业名为$jobName 的brokers没有配置！！ ")
    if (numThreads == 0)throw new Exception(s"realtime.json中作业名为$jobName 的numThreads没有配置！！ ")
    if (eventNum == 0)throw new Exception(s"realtime.json中作业名为$jobName 的numThreads没有配置！！ ")
    FlumePollContext(numThreads,eventNum,t.head)
  }


//获取topic partition offset 对应的关系
  def getTopicAndPartitionAndOffsets(topics:MessageList): Map[TopicAndPartition,Long] ={
    val topicsMo = topics.toList.map(_.asInstanceOf[MessageObjectImpl])
    topicsMo.flatMap(t=>{
      val tName = t.getString(InitContext.NAME)
      val psConf = t.getString(InitContext.PARTITIONS)//配置文件中的partitions
      var ps:Array[String] = Array[String]("0")
      if(psConf!=null&&psConf!=""){  //判断配置文件中是否配置
        ps = psConf.split(",")
      }
      val osConf = t.getString(InitContext.OFFSETS) //配置文件中的offset值
      var os =Array[String]("0")
      if(osConf!=null&&osConf!=""){//判断配置文件中是否配置
        os = t.getString(InitContext.OFFSETS).split(",")
      }
      val pos = ps.zip(os)
      pos.map(po=>{
        (TopicAndPartition(tName,po._1.toInt),po._2.toLong)
      })
    }).toMap
  }

  //获取配置文件中的topic解码器
  def getTopicsDecoder(topics:MessageList):Map[String,Decoder[Any]]={
    val topicsMo = topics.toList.map(_.asInstanceOf[MessageObjectImpl])
    val data:Map[String,String] = topicsMo.map(t=>{
      val tName = t.getString(InitContext.NAME)
      val decoderName= t.getString(InitContext.DECODER)
      if(null==decoderName||""==decoderName){
        (tName,"DefaultDecoder")
      }else{
        (tName,decoderName)
      }
    }).toMap
    DataUtils.getDecoders(data)
  }


  def getOrElse(key:String,default:String): String ={
    var str = job.getString(key)
    if(str==null||str==""){
      str = default
    }
    str
  }


  override def toString = s"JobContext(zookeeper:$zookeeper, brokers:$brokers, jobName:$jobName, source: $source , method:$method , groupId:$groupId, windowDuration:$windowDuration, slideDuration：$slideDuration, checkPath:$checkPath, task:$task, tpos:$tpos)"
}
object JobContext{
  def apply(
             job: MessageObjectImpl,
             task: List[TaskContext],
             envContext: EnvContext): JobContext = new JobContext(job, task,envContext)
}


