package cn.lsmsp.sparkframe.common.util

import java.text.SimpleDateFormat
import java.util.Date

import com.google.protobuf.TextFormat.ParseException

/**
  * Created by wangcongjun on 2017/7/10.
  */
object DateUtils {
  val pattern1 = "yyyy-MM-dd HH:mm:ss"
  val pattern2 = "yyyyMMddHHmmss"
  val pattern3 = "yyyyMMdd"



  private def getDateFormat(pattern:String):SimpleDateFormat={
    new SimpleDateFormat(pattern)
  }
  /**"yyyy-MM-dd HH:mm:ss"
    *
    * @return
    */
  def getCurrentFormatDate(pattern:String=pattern1): String ={
    convertDate2String(new Date(),pattern)
  }

  def convertDate2String(date:Date,pattern:String=pattern1): String ={
    getDateFormat(pattern).format(date)
  }

  def converString2Date(date:String,pattern:String=pattern1):Date={
    try{
      getDateFormat(pattern).parse(date)
    }catch {
      case _:Exception=> getDateFormat(pattern2).parse(date)
    }

  }
}
