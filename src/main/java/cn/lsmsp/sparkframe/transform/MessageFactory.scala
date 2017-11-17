package cn.lsmsp.sparkframe.transform

import org.apache.commons.io.IOUtils
import org.apache.commons.io.FileUtils
import java.io.File
import java.util

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.transform.message._

import scala.collection.JavaConverters._
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable

/**
  * Created by wangcongjun on 2017/5/16.
  */
object MessageFactory extends Logging {

  /********************************************************************
    * 创建MessageList对象
    ********************************************************************/
  def createMessageList(): MessageList = {
    new MessageListImpl()
  }

  /**
    * 通过java的List创建MessageList对象
    */
  def createMessageListFromJavaList(_list: util.List[Any]):MessageList= {
    new MessageListImpl(_list.asScala)
  }

  /**
    * 通过json字符串创建MessageList对象
    **/
  def getMessageListFromJson(json: String): MessageList = {
    val _list = new ObjectMapper().readValue(json, classOf[util.ArrayList[Any]])
    new MessageListImpl(_list.asScala)
  }
  /**
    * 通过不可变List创建MessageList对象
    */
  def createMessageListFromList(_list: List[Any]) {
    new MessageListImpl(_list.toBuffer)
  }

  /**********************************************************************************
    * 创建MessageObject对象
    **********************************************************************************/
  def createMessageObject(): MessageObject = new MessageObjectImpl()

  /**
    * 通过java的map集合创建MessageObject对象
    *
    * @return
    */
  def createMessageObjectFromJavaMap(_map: util.Map[String, Any]): MessageObject = {
    val map = mutable.Map[String, Any](_map.asScala.toMap.toSeq: _*)
    new MessageObjectImpl(map)
  }

  /**
    * json字符串转成messageObject
    */

  def getMessageObjectFromJson(json: String): MessageObject = {
    val mapper = new ObjectMapper()
    val map = mapper.readValue(json, classOf[java.util.Map[String, Any]]).asScala
    new MessageObjectImpl(map)
  }

  /**
    * 不可变Map转成messageObject
    */
  def getMessageObjectFromMap(map: mutable.Map[String, Any]): MessageObject = new MessageObjectImpl(mutable.Map[String, Any](map.toSeq: _*))

  /**
    * 普通对象转成MessageObject
    */

  def getMessageObjectFromObject(obj: Any): MessageObject = {
    MessageFactory.getMessageObjectFromMap(ObjectUtils.obj2Map(obj))
  }

  /**
  json文件转换成MessageObject
   */
  def getMessageObjectFromJsonFile(configName: String, absolute: Boolean): Message = {

    //    val file = new URL(configName)
    //    logInfo("file URL :" + file.toString())
    var jsonString: String = null

    //    val jsonString = IOUtils.toString(file)
    if (absolute) {
      jsonString = FileUtils.readFileToString(new File(configName))
    } else {
      val file = this.getClass.getResource("/" + configName)
      jsonString = IOUtils.toString(file)
    }
    if (jsonString.startsWith("{")) {
      MessageFactory.getMessageObjectFromJson(jsonString)
    } else {
      MessageFactory.getMessageListFromJson(jsonString)
    }
  }
}
