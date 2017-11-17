package cn.lsmsp.sparkframe.common.util

import org.apache.spark.rdd.RDD

object ClassUtils {
  
  //根据全类名调用指定的方法
  def callCalssMethod[T](className:String,methodName:String,args:Object,argclazz:Class[T]):Any={
    val clazz = Class.forName(className)
    val m = clazz.getMethod(methodName, argclazz)
    m.invoke(clazz.newInstance(), args)
    clazz.newInstance()
  }
}