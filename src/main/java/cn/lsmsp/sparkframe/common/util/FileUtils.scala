package cn.lsmsp.sparkframe.common.util

import java.util.Properties

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.realtime.InitContext
import cn.lsmsp.sparkframe.transform.message.MessageObject

object FileUtils extends Logging{
  def readJsonFile2Map(fileName:String,configName:String): Map[String,Any] ={
    val dbConfig = InitContext.getContextConfig(fileName).asInstanceOf[MessageObject]
    dbConfig.getObject(configName).toMap
  }

  /**
    *
    * @param fileName 文件名称
    * @param configName 配置文件中指定的配置名称
    * @return
    */
  def readJsonFile2Prop(fileName:String,configName:String): Properties ={
    val dbConfig = InitContext.getContextConfig(fileName).asInstanceOf[MessageObject]
    val mapConf = dbConfig.getObject(configName).toMap
    val conf = new Properties()
    mapConf.foreach(x=>conf.setProperty(x._1,x._2.toString))
    conf
  }
}
