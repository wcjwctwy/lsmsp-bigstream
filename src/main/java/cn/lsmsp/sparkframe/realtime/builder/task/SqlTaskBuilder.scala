package cn.lsmsp.sparkframe.realtime.builder.task

import cn.lsmsp.sparkframe.realtime.Business
import cn.lsmsp.sparkframe.realtime.builder.Builder
import cn.lsmsp.sparkframe.realtime.entity.TaskContext
/**
  * 获取sql的task
  * Created by wangcongjun on 2017/6/26.
  */
class SqlTaskBuilder(taskContext:TaskContext) extends Builder{
  override def create(): Business = {
    val className = taskContext.taskName
    Class.forName(className).newInstance().asInstanceOf[Business]
  }
}
object SqlTaskBuilder{
  def apply(taskContext: TaskContext): SqlTaskBuilder = new SqlTaskBuilder(taskContext)
}
