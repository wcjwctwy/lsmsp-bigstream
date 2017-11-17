package cn.lsmsp.sparkframe.realtime

import cn.lsmsp.sparkframe.realtime.adaptor.SparkContextAdaptor
import cn.lsmsp.sparkframe.realtime.builder.job.JobBuilder
import cn.lsmsp.sparkframe.realtime.builder.task.TaskBuilder
import cn.lsmsp.sparkframe.realtime.entity.{AppContext, JobContext}
import org.apache.spark.streaming.StreamingContext


/**
  * Created by wangcongjun on 2017/4/17.
  */
abstract class RealtimeBusi extends Business {
  /**
    * 初始化上下文
    * 1、获得任务名
    * 2、获取消费者的信息
    * 3、zk信息
    */

  private val appName = this.getClass.getSimpleName.replace("$", "")
  val appContext: AppContext = InitContext.getAppContext(appName)
  var ssc: StreamingContext = _

  /**
    * 业务入口
    * 创建直连kafka的DStream
    */
  override def start(): Unit = {
    ssc = SparkContextAdaptor.getSSC(appName)
    val jobs = appContext.jobs
    jobs.filter(_._2.status==2).foreach(job=>{
      //判断job是否为空
//      if (job.isEmpty) {
//        throw new Exception(s"当前job：$jobName 没有配置!!")
//      }
      logInfo(s"job: $job")
      run(job._2)
    })

    //启动流处理
    ssc.start()
    ssc.awaitTermination()
  }

  def run(job: JobContext): Unit = {
    logInfo(s"jobStart: ${job.jobName}: ${job.toString}")
    val jobDS = JobBuilder.create(appContext,job)
    TaskBuilder.create(job).foreach(business=>{
      business.business(jobDS)
    })
    jobDS.foreachRDD(rdd => {
      /**
        * 根据此次作业的名称jobName获取给作业下的所有的所有的业务处理实例
        * 对数据流中的每个rdd进行task业务处理
        */
      try {
        TaskBuilder.create(job).foreach(business => {
          business.business(rdd)
        })
      } catch {
        case e: Exception => logError("处理消息异常：", e)
      }
    })
  }
}

