package cn.lsmsp.sparkframe.realtime

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.transform.message.MessageObject

object ConfigAdaptor extends Logging {
  def convertTaskContext(mo:MessageObject):MessageObject={
    //获取task的上下文信息
    val master = Option(mo.getString(InitContext.MASTER))
    val zkAddr =Option( mo.getString(InitContext.ZK))
    val kafkaAddr =Option( mo.getString(InitContext.BROKERS))
    val context = InitContext.getContext.getObject(InitContext.FILENAME)
    if(!master.isDefined||master.get==""){
      mo.put(InitContext.MASTER,context.getString(InitContext.MASTER))
    }
    if(!zkAddr.isDefined||zkAddr.get==""){
      mo.put(InitContext.ZK,context.getString(InitContext.ZK))
    }
    if(!kafkaAddr.isDefined||kafkaAddr.get==""){
      mo.put(InitContext.BROKERS,context.getString(InitContext.BROKERS))
    }
    logInfo("Converted: "+mo.toString())
    mo
  }
}