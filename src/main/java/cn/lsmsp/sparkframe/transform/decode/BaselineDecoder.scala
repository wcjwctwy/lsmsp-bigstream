package cn.lsmsp.sparkframe.transform.decode

import java.io.UnsupportedEncodingException

import cn.lsmsp.sync.protobuf.ProtobufDecode
import cn.lsmsp.sync.protobuf.security.SecurityProtoBuf.LsAssetBaselineResult
import com.google.protobuf.InvalidProtocolBufferException
import cn.lsmsp.sparkframe.common.log.Logging
import kafka.serializer.Decoder

/**
  * Created by wangcongjun on 2017/4/14.
  */
class BaselineDecoder extends Decoder[String] with Logging {

  override def fromBytes(bytes: Array[Byte]): String = {
    try {
       ProtobufDecode.decode(bytes, classOf[LsAssetBaselineResult].getCanonicalName).toString
    } catch {
      case var6: InvalidProtocolBufferException =>
        logError(var6.getMessage)
        null
      case var7: UnsupportedEncodingException =>
        logError(var7.getMessage)
        null
    }
  }
}
