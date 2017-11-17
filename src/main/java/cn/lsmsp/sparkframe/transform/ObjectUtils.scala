package cn.lsmsp.sparkframe.transform

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.common.util.StringUtils

import scala.collection.JavaConverters._

/**
  * Created by wangcongjun on 2017/5/16.
  */
object ObjectUtils extends Logging{
  def GET:String = "get"

  def getFieldValue[T](obj:Any,fieldName:String): T ={
    //获取get方法
    val method = obj.getClass.getDeclaredMethod(GET+StringUtils.firstUpper(fieldName))
    //获取属性值
    method.invoke(obj).asInstanceOf[T]
  }
import scala.collection.mutable.Map
  def obj2Map(obj:Any):Map[String,Any] ={
    //获取类Class
    val clazz = obj.getClass
    val map:Map[String,Any]=Map[String,Any]()
    logInfo(clazz.toString)
    //得到全部的属性
    val fields = clazz.getDeclaredFields
    logInfo(fields.toList.toString())
    //转换属性为（属性名，属性值）
    val map1 = fields.foreach(field=>{
      val fieldtype = field.getType.getSimpleName
      val name = field.getName
      map += (name->getFieldValue[String](obj,name))
    })
    map
  }

}

