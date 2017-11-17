package cn.lsmsp.sparkframe.transform.encode

import java.util

import cn.lsmsp.dbms.alarm.entity.LsAlarm
import cn.lsmsp.sync.protobuf.ProtobufEncode
import org.apache.kafka.common.serialization.Serializer

class LsAlarmSerializer extends Serializer[LsAlarm]{



  override def configure(map: util.Map[String, _], b: Boolean): Unit = ???

  override def serialize(s: String, t: LsAlarm): Array[Byte] = {
    ProtobufEncode.encode(t).getBuf
  }

  override def close(): Unit = ???
}
