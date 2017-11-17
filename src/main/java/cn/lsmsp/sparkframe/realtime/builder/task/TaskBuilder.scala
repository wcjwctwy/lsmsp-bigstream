package cn.lsmsp.sparkframe.realtime.builder.task

import cn.lsmsp.sparkframe.realtime.{Business, InitContext}
import cn.lsmsp.sparkframe.realtime.builder.Builder
import cn.lsmsp.sparkframe.realtime.entity.JobContext

/**
  * 获取当前作业的所有的task
  * Created by wangcongjun on 2017/6/26.
  */
object TaskBuilder extends Builder {
  val bizs = scala.collection.mutable.Map[String, List[Business]]()

  override def create(jobContext: JobContext): List[Business] = {
    if (bizs.get(jobContext.jobName).isEmpty) {
      val excuters = jobContext.task
        .filter(_.status == InitContext.ACTIVITYTASK)
      .map(taskContext => {
        val sql = Option(taskContext.sql)
        if (sql.isEmpty) {
           DefaultTaskBuilder(taskContext).create()
        } else {
          SqlTaskBuilder(taskContext).create()
        }
      })
      bizs+=jobContext.jobName->excuters
    }
    bizs(jobContext.jobName)
  }
}

//object TaskBuilder{
//  def apply( jobContext:JobContext): TaskBuilder = new TaskBuilder(jobContext)
//}
