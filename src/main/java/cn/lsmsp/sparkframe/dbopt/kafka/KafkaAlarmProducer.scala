package cn.lsmsp.sparkframe.dbopt.kafka

import cn.lsmsp.dbms.alarm.entity.LsAlarm
import cn.lsmsp.sparkframe.common.util.FileUtils
import cn.lsmsp.sparkframe.transform.encode.LsAlarmSerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer


object KafkaAlarmProducer {
  var _alarmProducer: KafkaProducer[String,LsAlarm]=_
  val topic = "lsmsp-E2C-Gather-alarm"
  def alarmProducer: KafkaProducer[String,LsAlarm] ={
    if(_alarmProducer==null){
      val kconf = FileUtils.readJsonFile2Prop("dbconfig","kafka")
      _alarmProducer=new KafkaProducer[String,LsAlarm](kconf,new StringSerializer(),new LsAlarmSerializer())
    }
    _alarmProducer
  }
  def senMsg(alarm:LsAlarm ): Unit ={
  val pr = new ProducerRecord[String,LsAlarm](topic,alarm)
  alarmProducer.send(pr)
}
}
