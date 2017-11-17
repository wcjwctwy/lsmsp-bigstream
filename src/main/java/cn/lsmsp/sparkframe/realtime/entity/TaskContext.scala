package cn.lsmsp.sparkframe.realtime.entity

import cn.lsmsp.sparkframe.realtime.InitContext
import cn.lsmsp.sparkframe.transform.message.MessageObjectImpl

/**
  * Created by wangcongjun on 2017/6/29.
  */
class TaskContext(task:MessageObjectImpl) extends Context {
  var taskName: String=task.getString(InitContext.TASKNAME)
  var status: Int=task.getInt(InitContext.STATUS)
  var  sql: String=task.getString(InitContext.SQL)
  var srcPath:String=task.getString(InitContext.SRCPATH)
  var destPath:String=task.getString(InitContext.DESTPATH)
}

object TaskContext{
  def apply(task: MessageObjectImpl): TaskContext = new TaskContext(task)
}
