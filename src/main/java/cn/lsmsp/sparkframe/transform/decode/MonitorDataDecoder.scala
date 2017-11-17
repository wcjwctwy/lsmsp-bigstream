package cn.lsmsp.sparkframe.transform.decode

//import cn.lsmsp.dbms.network.entity.ConcernsDataResultSet
import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sync.protobuf.ProtobufDecode
import com.google.protobuf.InvalidProtocolBufferException
import kafka.serializer.Decoder

/**
  * Created by wangcongjun on 2017/4/14.
  */
class MonitorDataDecoder extends Decoder[String] with Logging{
  override def fromBytes(bytes: Array[Byte]): String = {
//    try
//      //cn.lsmsp.dbms.network.entity.ConcernsDataResultSet
//      ProtobufDecode.decode(bytes, classOf[ConcernsDataResultSet].getCanonicalName).toString
//    catch {
//      case var7: InvalidProtocolBufferException =>
//        logError(var7.getMessage)
//        null
//    }
    null
  }
}
