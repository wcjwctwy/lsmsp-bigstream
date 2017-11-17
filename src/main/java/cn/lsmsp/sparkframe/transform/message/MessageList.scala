package cn.lsmsp.sparkframe.transform.message

import scala.collection.mutable.ArrayBuffer
/**
  * Created by wangcongjun on 2017/4/17.
  */
trait MessageList extends Message{
  def size():Int
  def add(value:Any):MessageList
  def del(key:Int)
  def getObject(index:Int):MessageObject
  def get(index:Int):Any
  def get(index:String):Any
  def getList(index:Int):MessageList
  def toList:List[Any]
  def getComplex(keys: ArrayBuffer[String]): Any
  def toMessageObject: MessageObject
  def pop(key:Int):Any
}
