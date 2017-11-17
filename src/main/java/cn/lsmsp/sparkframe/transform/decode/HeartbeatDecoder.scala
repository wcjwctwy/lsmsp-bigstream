package cn.lsmsp.sparkframe.transform.decode

import java.io.UnsupportedEncodingException

//import cn.lsmsp.dbms.system.entity.TbCollectorStatus
import cn.lsmsp.sparkframe.common.log.Logging
import cn.lsmsp.sync.protobuf.ProtobufDecode
import com.google.protobuf.InvalidProtocolBufferException
import kafka.serializer.Decoder

/**
  * Created by wangcongjun on 2017/4/14.
  */
class HeartbeatDecoder extends Decoder[String] with Logging{
  override def fromBytes(bytes: Array[Byte]): String = {

//    try {
//       ProtobufDecode.decode(bytes, classOf[TbCollectorStatus].getCanonicalName).toString
//    } catch {
//      case var5: InvalidProtocolBufferException =>
//        logError(var5.getMessage)
//        null
//      case var6: UnsupportedEncodingException =>
//        logError(var6.getMessage)
//        null
//    }
    null
  }
}
