package cn.lsmsp.sparkframe.transform.message

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangcongjun on 2017/4/17.
  */
trait MessageObject extends Message{
  def size():Int
  def getString(key:String):String
  def getInt(key:String):Int
  def getLong(key:String):Long
  def getFloat(key:String):Float
  def getObject(key:String):MessageObject
  def getList(key:String):MessageList
  def put(key:String,value:Any)
  def del(key:String)
  def toMap:Map[String,Any]
  def get(key:String):Any
  def getComplex(keys: ArrayBuffer[String]): Any
  def getSimple(key: String): Any
  def addObject(mo: MessageObject): MessageObject
  def pop(key:String): Any
//  def flat(): List[MessageObject]
}
