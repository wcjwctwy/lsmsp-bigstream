package cn.lsmsp.sparkframe.common.util

/**
  * Created by wangcongjun on 2017/5/16.
  * 操作字符串的工具类
  */
object StringUtils {
  def firstUpper(str:String): String ={
    str.substring(0,1).toUpperCase().concat(str.substring(1))
  }

  def main(args: Array[String]): Unit = {
    print("")
  }
}
