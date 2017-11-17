package cn.lsmsp.sparkframe.transform.message

import java.util

import cn.lsmsp.sparkframe.common.util.ArrayUtil

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.fasterxml.jackson.databind.ObjectMapper


class MessageObjectImpl() extends MessageObject {
  private var map: mutable.Map[String, Any] = mutable.Map[String, Any]()

  /**
    * 构造方法：以Map来构造Mo对象
    */
  def this(_map: mutable.Map[String, Any]) = {
    this()
    map = _map
  }

  /**
    * 通过key获取String
    */
  override def getString(key: String): String = {
    try
      get(key).toString
    catch {
      case _: Exception => null
    }
  }

  /**
    * 获取指定节点的MessageList对象
    *
    * @return
    */
  override def getList(key: String): MessageList = {
    get(key) match {
      case value: java.util.List[Any@unchecked] => new MessageListImpl(value.asScala)
      case value: MessageList => value
      case _ => null
    }
  }

  /**
    * 获取指定节点的MessageObject对象
    *
    * @return
    */
  override def getObject(key: String): MessageObject = {
    try get(key).asInstanceOf[MessageObject]
    catch {
      case e: Exception =>
        logError("", e)
        null
    }
  }

  /**
    * 通过key获取value
    * 1、如果是一个简单的key则调用getSimple
    * 2、如果是一个路径则调用getComplex
    */
  override def get(key: String): Any = {
    val keys = ArrayBuffer[String]() ++ key.split("/")
    if (keys.nonEmpty) getComplex(keys) else if (keys.size == 1) getSimple(key) else null
  }

  /**
    * 简单的object调用
    * key的结构简单，只有一个或一层
    */
  def getSimple(key: String): Any = {
    try {
      map(key) match {
        case value: util.Map[String@unchecked, Any@unchecked] => new MessageObjectImpl(value.asScala)
        case value: util.List[Any@unchecked] => new MessageListImpl(value.asScala)
        case value: Any => value
      }
    } catch {
      case _: Exception =>
        //        logWarning(s"$key doesn't exist")
        null
    }
  }

  /**
    * 简单的object调用
    * key的结构复杂，有多层
    */
  def getComplex(keys: ArrayBuffer[String]): Any = {
    val _key = ArrayUtil.pop(keys)
    //当入参key是一个路径时
    if (keys.nonEmpty) {
      //判断下一个节点是Object还是List
      get(_key) match {
        case a: MessageObject => a.getComplex(keys)
        case a: MessageList => a.getComplex(keys)
      }
    } else {
      getSimple(_key)
    }
  }

  override def getInt(key: String): Int = {
    try {
      val str = getString(key)
      if (str == null) {
        0
      }else{

        str.toInt
      }
    }
    catch {
      case e: Exception =>
        logError("numtransform exception", e)
        0
    }
  }

  /**
    * 将MessageObject对象转换成Map
    *
    * @return
    */
  override def toMap: Map[String, Any] = {

    map.map(x => {
      x._2 match {
        case a: util.Map[String@unchecked, Any@unchecked] => (x._1, new MessageObjectImpl(a.asScala))
        case a: util.List[Any@unchecked] => (x._1, new MessageListImpl(a.asScala))
        case _ => x
      }
    }).toMap[String, Any]

  }

  override def getFloat(key: String): Float = {
    try
      getString(key).toFloat
    catch {
      case _: Exception =>
        logError("numtransform exception")
        0
    }
  }

  override def getLong(key: String): Long = {
    try
      getString(key).toLong
    catch {
      case _: Exception => 0L
    }
  }

  /**
    * 添加数据
    *
    */
  override def put(key: String, value: Any): Unit = {
    map.put(key, value)
  }

  def addObject(mo: MessageObject): MessageObject = {
    map ++= mo.toMap
    this
  }

  override def del(key: String): Unit = {
    map.remove(key)
  }

  def pop(key: String): Any = {
    val value = get(key)
    del(key)
    value
  }

  override def toString: String = new ObjectMapper().writeValueAsString(map.asJava)

  override def size(): Int = map.size
}
