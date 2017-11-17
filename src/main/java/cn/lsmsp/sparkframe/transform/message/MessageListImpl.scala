package cn.lsmsp.sparkframe.transform.message

import java.util

import cn.lsmsp.sparkframe.common.util.ArrayUtil
import cn.lsmsp.sparkframe.transform.MessageFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import com.fasterxml.jackson.databind.ObjectMapper

import scala.collection.mutable

/**
  * Created by wangcongjun on 2017/4/17.
  */
class MessageListImpl extends MessageList {

  override def size(): Int = list.size

  private val TAIL = -1
  var list: mutable.Buffer[Any] = mutable.Buffer[Any]()

  /**
    * 通过ListBuffer创建MessageList对象
    */
  def this(_list: mutable.Buffer[Any]) {
    this
    list = _list
  }

  /**
    * 向List中添加元素
    * @return
    */
  override def add(value: Any): MessageList = {
    list += value
    this
  }


  /**
    * 获取指定节点的MessageObject对象
    * @return
    */
  override def getObject(index: Int): MessageObject = {
    try
      get(index).asInstanceOf[MessageObject]
    catch {
      case e: Exception =>
        logError("", e)
        null
    }
  }

  /**
    * 获取指定节点的MessageList对象
    * @return
    */
  override def getList(index: Int): MessageList = {
    try
      get(index).asInstanceOf[MessageList]
    catch {
      case e: Exception =>
        logError("", e)
        null
    }
  }

  /**
    * 删除指定元素
    */
  override def del(key: Int): Unit = {
    list.remove(key)
  }

  /**
    * 弹出指定元素，即获得该元素值，并且在源数据中删除该元素
    * @return
    */
  def pop(key: Int): Any = {
    val value = list(key)
    del(key)
    value
  }


  /**
    * 通过key获取value
    * 1、如果是一个简单的key则调用get(Int)
    * 2、如果是一个路径则调用getComplex
    */
  override def get(key: String): Any = {
    val keys = ArrayBuffer[String]() ++ key.split("/")
    if (keys.nonEmpty) getComplex(keys) else if (keys.size == 1) get(key.toInt) else null
  }

  /**
    * 简单的object调用
    * key的结构简单，只有一个或一层
    * -1代表取值最后一个
    */
  override def get(index: Int): Any = {
    var _index = index
    try {
      if (index == TAIL) {
        _index = this.toList.size - 1
      }
      list(_index) match {
        case value:util.Map[String @unchecked,Any @unchecked] =>new MessageObjectImpl(value.asScala)
        case value: util.List[Any@unchecked] => new MessageListImpl(value.asScala)
        case value: Any => value
      }
    } catch {
      case _: Exception => logError(s"${_index} doesn't exist")
        null
    }
  }

  /**
    * 简单的object调用
    * key的结构复杂，有多层
    */
  override def getComplex(keys: ArrayBuffer[String]): Any = {
    val _key = ArrayUtil.pop(keys)
    //当入参key是一个路径时
    if (keys.nonEmpty) {
      //判断下一个节点是Object还是List
      get(_key.toInt) match {
        case value: MessageObject => value.getComplex(keys)
        case value: MessageList => value.getComplex(keys)
        case _ => logError("下一节点不是object或list")
      }
    } else {
      get(_key.toInt)
    }
  }

  /**
    * 将list中的数据全部转换为Message对象
    * @return
    */
  override def toList: List[Any] = {
    list.map({
      case value:util.Map[String@unchecked,Any@unchecked] =>new MessageObjectImpl(value.asScala)
      case value: util.List[Any@unchecked] => new MessageListImpl(value.asScala)
      case a: String => a
    }).toList
  }

  /**
    * 将MessageList对象转换成MessageObject
    * @return
    */
  def toMessageObject: MessageObject = {
    val map = list.map(x => {
      (list.indexOf(x).toString, x)
    }).toMap[String, Any]
    MessageFactory.getMessageObjectFromMap(scala.collection.mutable.Map[String, Any](map.toSeq: _*))
  }

  override def toString: String = new ObjectMapper().writeValueAsString(list.asJava)
}
