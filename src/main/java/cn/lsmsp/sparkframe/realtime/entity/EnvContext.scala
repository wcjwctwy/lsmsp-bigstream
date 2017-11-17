package cn.lsmsp.sparkframe.realtime.entity

import cn.lsmsp.sparkframe.realtime.InitContext
import cn.lsmsp.sparkframe.transform.message.MessageObjectImpl

/**
  * Created by wangcongjun on 2017/6/29.
  */
class EnvContext(env: MessageObjectImpl) extends Context {


  val zookeeper:String = env.getString(InitContext.ZK)
  val brokers:String = env.getString(InitContext.BROKERS)
  val master:String = env.getString(InitContext.MASTER)


}

object EnvContext{
  def apply(env: MessageObjectImpl): EnvContext = new EnvContext(env)
}
