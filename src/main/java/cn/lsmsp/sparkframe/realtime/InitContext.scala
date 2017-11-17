package cn.lsmsp.sparkframe.realtime

import java.io.File

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.realtime.entity._
import cn.lsmsp.sparkframe.transform.{DataUtils, MessageFactory}
import cn.lsmsp.sparkframe.transform.message._
import scala.collection.JavaConverters._

/**
  * Created by wangcongjun on 2017/4/18.
  */
object InitContext extends Logging {

  val EVENTNUM="eventNum"
  val OFFSETRESET="offsetReset"
  val NUMTHREADS="numThreads"
  val FILENAME = "realtime" //XML的文件名
  //  val COSUMWER = "consumer" //消费者
  val BROKERS = "brokers" //kafka集群地址
  val TOPICLIST = "topics" //消费的topic的list
  val PARTITIONS = "partitions" //topic中的partitions
  val OFFSETS = "offsets" //对应的partitions的offsets
  val NAME = "name" //topic 的name
//  val APPNAME = "appname" //任务名称
  val MASTER = "master" //spark集群的master
  val GROUPID = "groupId" //消费组的名称
  val PERIOD = "period" //sparkStreaming 的消费间隔
  val ZK = "zookeeper" //offet存储的zk地址
  val CONFIG = "config.properties" //程序初始化配置文件
  val RUNTIMEINFO = "runtimeinfo.properties" //运行时参数配置
  val EXCUTESERVER = "executeserver" //执行任务列表
  val DECODER = "decoder"
  val WIDDOWDURATION = "windowDuration"
  val SLIDEDURATION = "slideDuration"
  val CHECKPATH = "checkpath"
  val JOBS = "jobs"
  val JOBNAME = "jobName"
  val LOGLEVEL = "loglevel"
  val SQL = "sql"
  val ACTIVITYTASK = 2
  val EXCUTTESTATUS = "status"
  val TASKNAME = "taskName"
  val SRCPATH = "srcpPath"
  val DESTPATH = "destPath"
  val STATUS = "status"
  val SOURCE = "source"
  val METHOD= "method"

  private var dataContext: MessageObjectImpl = new MessageObjectImpl()
  private var appContext: AppContext = _

  /**
    * 初始化AppContext
    *
    * @return
    */
  def getAppContext(appName: String): AppContext = {
    if (appContext == null) {
      val config = getContextConfig(FILENAME).asInstanceOf[MessageObjectImpl]
      logInfo(config.toString)
      //初始化ENV配置
      val envContext = EnvContext(config)
      //读取task任务列表
      val excutes = getContextConfig(EXCUTESERVER).asInstanceOf[MessageObjectImpl]
      val tasks: Map[String, List[TaskContext]] = excutes.toMap.map(nts => {
        val jobName = nts._1
        val taskContexts: List[TaskContext] = nts._2.asInstanceOf[MessageListImpl].toList.map(_.asInstanceOf[MessageObjectImpl]).map(task => {
          new TaskContext(task)
        })
        (jobName, taskContexts)
      })
      //获取jobContext
      val jobList = config.getObject(appName).getList(JOBS)
      if (jobList != null) {
        val jobs = jobList.toList.map(_.asInstanceOf[MessageObjectImpl])
          .map(job => {
            val jobName = job.getString(InitContext.JOBNAME)
            //          val source = job.getString(InitContext.SOURCE)
            //获取解码器
            val jobContext = JobContext(job, tasks.getOrElse(jobName, List[TaskContext]()), envContext)
            (jobName, jobContext)
          }).toMap


        //      val data = getContextConfig(FILENAME).asInstanceOf[MessageObjectImpl].getObject(InitContext.DECODER)
        //      val decoders = DataUtils.getDecoders(data)
        //获取AppContext
        appContext = AppContext(appName, envContext, config, jobs)
      }
    }
    appContext
  }



  def getContext: MessageObjectImpl = {
    if (dataContext.toMap.size < 3) {
      getRuntimeInfo(CONFIG)
      getContextConfig(EXCUTESERVER)
      getContextConfig(FILENAME)
    }
    dataContext
  }

  /**
    * 读取json文本文件转为MessageObject
    */
  def getContextConfig(fileName: String): Message = {
    if (Option(dataContext.get(fileName)).isEmpty) {
      val path = getRuntimeInfo("config.properties").getString("localpath") + "/" + fileName + ".json"
      var msg: Message = null
      //如果文件在path路径下存在
      if (new File(path).exists()) {
        msg = MessageFactory.getMessageObjectFromJsonFile(path, true)
      } else {
        //文件不在path路径下
        msg = MessageFactory.getMessageObjectFromJsonFile(fileName + ".json", false)
      }
      dataContext.put(fileName, msg)
    }
    dataContext.get(fileName).asInstanceOf[Message]

  }

  /**
    * 读取运行配置runtime.properties
    * 把获取的properties对象转换成Mo对象
    * 存入dataContext
    */

  private def getRuntimeInfo(fileName: String): MessageObject = {
    if (Option(dataContext.get(fileName)).isEmpty) {
      val map = Map[String, String]()
      import scala.collection.mutable
      val prop = DataUtils.readPropFile(fileName).asScala.toSeq
      dataContext.put(fileName, new MessageObjectImpl(mutable.Map[String, Any](prop: _*)))
    }
    dataContext.getObject(fileName)
  }


}
