package cn.lsmsp.sparkframe.transform.decode

import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sparkframe.transform.decode.adaptor.DecoderAdaptor
import kafka.serializer.Decoder
import org.apache.spark.sql.types._

/**
  * 事件日志转换类：转换成map
  *
  * Created by wangcongjun on 2017/4/14.
  */
class EventDecoder(adaptor: DecoderAdaptor[Map[String, String]]) extends Decoder[Map[String, Any]] with Logging {
  override def fromBytes(bytes: Array[Byte]): Map[String, Any] = {
    val entity = adaptor.adaptorDecoder(bytes)
    //println(entity)
    entity
      .map(x => {
        x._1 match {
          case string: String if EventMeta.string2Long.contains(string) => try {
            (string, x._2.toString.toLong)
          } catch {
            case e: Exception => logError(s"字段名称：$string |字段值: ${x._2} 数据解码失败：${e.getMessage}")
              throw e
          }
          case string: String if EventMeta.string2Int.contains(string) => try {
            (string, x._2.toString.toInt)
          } catch {
            case e: Exception => logError(s"字段名称：$string |字段值: ${x._2} 数据解码失败：${e.getMessage}")
              throw e
          }
          case _ => (x._1, x._2.toString)
        }
      })
  }
}

/**
  * event数据的元数据；event映射成表时 表的结构和表的字段的类型  全部定义在这里
  */
case object EventMeta {
  val string2Long: Array[String] = "oldfilecreatetime,oldfilemodifytime,oldfileaccresstime, collectreceipttime,socreceipttime,eventstarttime,eventendtime,filecreatetime,filemodifytime,fileaccresstime,devicereceipttime".split(",")
  //  val byteString2Int:Array[String]="eventtype,isanalyzerevent".split(",")
  val string2Int: Array[String] = "eventtype,isanalyzerevent,eventcount".split(",")
  private val strings = "ident,sid,cusid,collectname,eventtype,analyzerid,isanalyzerevent,eventname,eventmessage,eventcount,eventlevel,eventresult,collectidlist,extendid,issourceattacker,eventcategory,eventcategorybehavior,eventcategorytechnique,categorydevice,attackfashionlevel,eventfeature,eventcategoryobject,bytesin,bytesout,devicereceipttime,collectreceipttime,socreceipttime,eventstarttime,eventendtime,srcaddress,srctype,srcdnsdomain,srchostname,srcmac,srcnatdomain,srcport,srcprocessname,srcservicename,srcinterfaceisspoofed,srcinterface,srctransaddress,srctransport,srctranszone,srcuserid,srcusername,srcuserprivileges,srcusertype,srczone,taraddress,tartype,tardnsdomain,tarhostname,tarmac,tarnatdomain,tarport,tarprocessname,tarservicename,tarinterfaceisspoofed,tarinterface,tartransaddress,tartransport,tartranszone,taruserid,tarusername,taruserprivileges,tarusertype,tarzone,appprotocol,transprotocol,filepath,filename,filecreatetime,filemodifytime,fileaccresstime,filesize,disksize,filesystemtype,fileid,filehash,filetype,fileaccressprivileges,oldfilepath,oldfilename,oldfilecreatetime,oldfilemodifytime,oldfileaccresstime,oldfilesize,olddisksize,oldfilesystemtype,oldfileid,oldfilehash,oldfiletype,oldfileaccressprivileges,deviceaddress,devicetransaddress,deviceaction,devicednsdomain,devicedomain,devicehostname,devicemac,deviceeventlevel,deviceinboundinterface,deviceoutboundinterface,devicetransdomain,deviceprocessname,deviceproduct,devicetimezone,devicezone,devicetranszone,reqclientapp,httpreqcontext,httpcookie,reqmethod,repcode,apppath,appparam,appvariable,reqname,requrl,reqaction,referenceorigin,referencemeaning,referencename,referenceurl,analyzereventidlist,rawevent,entid".split(",")
  val dic: Map[String, Int] = strings.zip(0 to strings.length).toMap
  val schema =
    StructType(
      strings.map {
        case fieldName: String if string2Long.contains(fieldName) => StructField(fieldName, LongType)
        case fieldName: String if string2Int.contains(fieldName) => StructField(fieldName, IntegerType)
        case fieldName: Any => StructField(fieldName, StringType)
      })
}