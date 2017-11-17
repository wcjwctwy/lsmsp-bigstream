package cn.lsmsp.sparkframe.realtime.offset

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.realtime.entity.TopicInfo
import kafka.common.TopicAndPartition
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNodeExistsException

import scala.collection.mutable
import org.apache.spark.streaming.kafka.OffsetRange

/**
  * Created by wangcongjun on 2017/4/17.
  */
class OffsetOptImpl(val servers: String) extends OffsetOpt with Logging {
  private val zkClient: ZookeeperHelper = ZookeeperHelper(servers)
  private val OFFSET = -1L //offset是否自定义

  //关闭zk连接
  /**
    * 检查offset存储的path是否存在若不在则新建并且赋值为0L
    */
  override def checkoffsetPath(groupId: String, topicInfos: List[TopicInfo]): Unit = {
    topicInfos.foreach(topic => {
      topic.parOffs.foreach(po => {
        val zkPath = s"/$groupId/${topic.tName}/${po._1}/offset"
        if (!zkClient.exists(zkPath)) {
          creatPath(zkPath)
        }
      })
    })
  }

  /**
    * offset的优先级问题  在处理offset时有三种选择
    * 1、kafka的offset值
    * 2、zookeeper的offset值
    * 3、配置文件的offset值
    * 当zookeeper上没有值得时候，以配置文件为准 配置文件上也没有则以kafka上的offset为准
    */
  override def offsetPriority(
                               groupId: String,
                               topicInfos: Map[TopicAndPartition, Long],
                               brokers: String): Map[TopicAndPartition, Long] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    //println(kafkaParams.toBuffer)
    val tps = topicInfos.keys.toSet
    /*
    获取kafka的offset值
     */
    val helper = new KafkaClusterHelper(kafkaParams)
    val earliset = helper.getEarliestLeaderOffsets(tps)
    val latest = helper.getLatestLeaderOffsets(tps)

    /*
      比较kafka zookeeper config的三者的offset
     */

    topicInfos.map(topic => {
        val zkPath = s"/$groupId/${topic._1.topic}/${topic._1.partition}/offset"
      if (!zkClient.exists(zkPath)) {
        creatPath(zkPath)
      }
        var offset: Long = 0L
        //获取zk上的offset值
        val zkOffset = Option(zkClient.readData(zkPath))
        //获取kafka上的offset值
        val kafkaMax = latest.right.get(topic._1).offset
        val kafkaMin = earliset.right.get(topic._1).offset
        logInfo(s"configOffset:${topic._2}|kafkaOffset:$kafkaMax,$kafkaMin |ZKoffset:$zkOffset")
        //选择offset值
        if (topic._2 == OFFSET) if (zkOffset.isDefined) offset = zkOffset.get else offset = kafkaMax else offset = topic._2
        //与比较kafka上的最大值和最小值
        if (offset > kafkaMax) {
          logWarning(s"kafka's max value is $kafkaMax but consumer's offset is $offset")
          (topic._1, kafkaMax)
        } else if (offset < kafkaMin) {
          logWarning(s"kafka's min value is $kafkaMin but consumer's offset is $offset")
          (topic._1, kafkaMin)
        } else (topic._1, offset)
    })
  }

  override def offsetValid(path: String, maxOffset: Long): Boolean = {
    if (!zkClient.exists(path)) {
      creatPath(path)
      zkClient.writeData(path, 0L)
    } else {
      val zkOffset = zkClient.readData[Long](path)
      //zk上的大于kafka
      if (zkOffset > maxOffset) {
        logWarning(s"the offset's value of zookeeper is $zkOffset but kafka's $maxOffset  ")
        zkClient.writeData(path, maxOffset)
      }
      //等于或小于不做修改
    }
    true
  }

  override def creatPath(path: String): Unit = {
    logInfo(path)
    //校验path的合法性
    if (path == null) {
      logError("Create path cannot be empty ！")
      return
    }
    if (!path.startsWith("/")) {
      logError("The path must be created with / at the beginning")
      return
    }
    //遍历paths逐级创建
    path.substring(1).split("/").foldLeft("/")(op = (x, y) => {
      val temppath = x + y
      try {
        this.synchronized(if (!zkClient.exists(temppath)) {
          zkClient.createPersistent(temppath)
        })
      } catch {
        //节点存在不抛异常
        case e: ZkNodeExistsException =>
          val msg = "节点已存在"
          logWarning(msg)
        case e: Exception =>
          val msg = s"$temppath 节点创建失败" + e
          logWarning(msg)
      }
      temppath + "/"
    })
  }

  def delPath(path: String, isEmpty: Boolean): Unit = {
    try
        if (isEmpty) zkClient.delete(path)
        else zkClient.deleteRecursive(path)
    catch {
      case e: Exception =>
        logError(path + "节点删除失败" + e)
    }
  }

  //保存offset
  def saveOffset(offsetsList: Array[OffsetRange], groupId: String): Unit = {
    offsetsList.foreach(o => {
      //      logInfo(s"============topic : ${o.topic} |partition : ${o.partition} |startoffset : ${o.fromOffset} |maxoffset :  ${o.untilOffset}")
      val offsetPath = s"/$groupId/${o.topic}/${o.partition}/offset"
      zkClient.writeData(offsetPath, o.untilOffset)
    })
  }
}

object OffsetOptImpl {
  val connects: mutable.Map[String, OffsetOptImpl] = mutable.Map[String, OffsetOptImpl]()

  implicit def thisZK2Zk(offsetOptImpl: OffsetOptImpl): ZkClient = offsetOptImpl.zkClient

  def apply(servers: String): OffsetOptImpl = {
    if (!connects.contains(servers)) {
      connects += (servers -> new OffsetOptImpl(servers))
    }
    connects(servers)
  }
}