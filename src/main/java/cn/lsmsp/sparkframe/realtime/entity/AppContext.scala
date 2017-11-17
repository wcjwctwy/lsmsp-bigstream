package cn.lsmsp.sparkframe.realtime.entity

import cn.lsmsp.sparkframe.realtime.InitContext
import cn.lsmsp.sparkframe.transform.message.MessageObjectImpl

/**
  * Created by wangcongjun on 2017/6/29.
  */
class AppContext(_appName: String,
                 envContext: EnvContext,
                 app: MessageObjectImpl,
                 _jobs: Map[String, JobContext]
                ) extends Context {

  val appName: String = _appName
  val master: String = getOrElse(InitContext.MASTER,envContext.master)
  val period: Int = app.getObject(appName).getInt(InitContext.PERIOD)
  val jobs: Map[String, JobContext] = _jobs

  def getOrElse(key: String,default:String): String = {
    var str = app.getString(key)
    if (str == null || str == "") {
      str = default
    }
    str
  }


  override def toString = s"AppContext(master:$master, period:$period, jobs:$jobs)"
}

object AppContext {
  def apply(appName: String,
            envContext: EnvContext,
            app: MessageObjectImpl,
            jobs: Map[String, JobContext]
           ): AppContext = new AppContext(appName, envContext, app, jobs)
}
