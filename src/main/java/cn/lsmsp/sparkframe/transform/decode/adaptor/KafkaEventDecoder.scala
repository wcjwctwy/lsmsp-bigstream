package cn.lsmsp.sparkframe.transform.decode.adaptor

import cn.lsmsp.sync.protobuf.alarm.AlarmProtoBuf
import com.google.protobuf.ByteString

import scala.collection.JavaConverters._
import com.google.protobuf.Descriptors.FieldDescriptor

/**
  * Created by wangcongjun on 2017/7/6.
  */
class KafkaEventDecoder extends DecoderAdaptor[Map[String, String] ] {
  val byteString: Array[String] = "eventtype,isanalyzerevent".split(",")

  override def adaptorDecoder(bytes: Array[Byte]): Map[String, String] = {
    val entity = AlarmProtoBuf.SeclogMain.parseFrom(bytes).getAllFields.asScala
//    logDebug(entity.map(_._1.getName).toString())
    entity.map(msg => {
      msg._1 match {
        case fd: FieldDescriptor if byteString.contains(fd.getName) => (msg._1.getName.toLowerCase, msg._2.asInstanceOf[ByteString].toStringUtf8)
        case fd: FieldDescriptor => (fd.getName.toLowerCase, msg._2.toString)
      }

    }).toMap
  }
}

