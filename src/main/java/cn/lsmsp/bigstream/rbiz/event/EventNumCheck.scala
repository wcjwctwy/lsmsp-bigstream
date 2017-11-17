package cn.lsmsp.bigstream.rbiz.event

import java.util.Date

import cn.lsmsp.dbms.alarm.entity.LsAlarm
import cn.lsmsp.sparkframe.common.util.DateUtils
import cn.lsmsp.sparkframe.dbopt.kafka.KafkaAlarmProducer
import cn.lsmsp.sparkframe.dbopt.mysql.StreamingMySqlContext
import cn.lsmsp.sparkframe.dbopt.utils.SqlUtils
import cn.lsmsp.sparkframe.realtime.Business
import org.apache.spark.rdd.RDD

case class LogCheckEntity(entId: String, assetId: String) {
  override def toString: String = s"entId-$entId|assetId-$assetId"
}


class EventNumCheck extends Business {

  val NEWMEMB = 0
  val HAS2NONE = 1
  val ALWAYSNONE = -1
  val NONE2HAS = 2
  val ALWAYAHAS = -2

  /**
    * LogCheckEntity 表示唯一的企业资产
    * Tuple3[Int,Int,Int]
    * 1、表示上次记录的数量
    * 2、表示记录的状态(-1,0,1,2)   0，表示新添加 1：表示上次有这次没有 -1：表示一直没有 2：表示上次没有这次有了 -2：表示一直有
    * 3、状态持续的次数
    */
  val AssetLogStats: scala.collection.mutable.Map[LogCheckEntity, Tuple3[Int, Int, Int]]
  = scala.collection.mutable.Map[LogCheckEntity, Tuple3[Int, Int, Int]]()

  val DEFAUTVALUE = "404"

  def getMysqlData(): Unit = {
    logInfo("=====================read database================")
    val conn = StreamingMySqlContext.getConnection
    val rs = SqlUtils.query(conn, "select * from view_collect_asset")
    while (rs.next()) {
      val lce = LogCheckEntity(rs.getObject(1).toString, rs.getObject(2).toString)
      if (!AssetLogStats.contains(lce)) {
        AssetLogStats += LogCheckEntity(rs.getObject(1).toString, rs.getObject(2).toString) -> (0, NEWMEMB, 0)
      }
    }
  }

  def alarmMsgHandler(msg: String, lce: LogCheckEntity): Unit = {
    println(msg)
    val alarm = new LsAlarm()
    alarm.setEntId(lce.entId.toLong)
    alarm.setAssetId(lce.assetId.toLong)
    alarm.setAlarmParseRule("")
    if("告警".equals(msg)){
      alarm.setContent("5分钟内未接收到kafka中安全事件")
    }else{
      alarm.setContent("接收到kafka中安全事件告警解除")
    }
    alarm.setEventRuleId("")
    //entity.setFromIp("");
    alarm.setCreatetime(new Date())
    alarm.setLasttime(new Date())
    alarm.setTitle("安全事件收集"+msg)
    alarm.setSeverity(3) // 3：告警

    alarm.setSubjectCode("888888") //具体性能指标的concernsId

    //entity.setEntId(MonitorMainService.entId);
    alarm.setSubjectNo(0) //具体发出告警的对象index

    alarm.setAlarmParseRule("")
    alarm.setCount(1) // 告警次数

    alarm.setCategory(5) // 5:业务系统

    alarm.setAttentionDepth(0)
    alarm.setOperate(0) //0:告警操作  无任何操作

    alarm.setStatus(0) // 0：待处理

    alarm.setAppStatus(0)
    KafkaAlarmProducer.senMsg(alarm)
    logInfo(msg)
  }

  override def business(rdd: RDD[(String, Any)]): Unit = {
    //****************worker**********************
    //统计出有日志的企业资产
    val countRdd = rdd.map(msg => {
      AssetLogStats.foreach(println)
      val msgs = msg._2.asInstanceOf[Map[String, Any]]
      log.info(s" className:${this.getClass.getSimpleName},hasCode:${this.hashCode()}")
      (LogCheckEntity(msgs.getOrElse("entid", DEFAUTVALUE).toString, msgs.getOrElse("cusid", DEFAUTVALUE).toString), 1)
    }).reduceByKey(_ + _).collect().toMap


    //****************driver端执行**********************
    //    读取数据库中的数据
    val nowTime = DateUtils.getCurrentFormatDate().split(" ")(1)
    if (AssetLogStats.isEmpty) {
      getMysqlData()
    } else if (nowTime > "08:00:00" && nowTime <= "10:10:00") {
      getMysqlData()
    }

    /**
      * 和数据库中的数据进行比较
      * 遍历AssetLogStats （上一次日志采集的状态）状态
      * countRdd 为当前的日志采集状态
      */
    AssetLogStats.foreach(stat => {
      log.info(s" className:${this.getClass.getSimpleName},hasCode:${this.hashCode()}")
      if (countRdd.contains(stat._1)) {
        //有日志的情况
        stat._2._2 match {
          //新添加的资产 第一有数据
          case NEWMEMB => AssetLogStats += stat._1 -> (countRdd(stat._1), ALWAYAHAS, 0)
          //上次没有日志 这次又了
          case HAS2NONE => {
            val msg = "告警解除"
            alarmMsgHandler(msg, stat._1)
            AssetLogStats += stat._1 -> (countRdd(stat._1), NONE2HAS, 0)
          }
          //一直没有日志这次又了
          case ALWAYSNONE => {
            val msg = s"告警解除"
            alarmMsgHandler(msg, stat._1)
            AssetLogStats += stat._1 -> (countRdd(stat._1), NONE2HAS, 0)
          }
          //之前刚有现在也有
          case NONE2HAS => AssetLogStats += stat._1 -> (countRdd(stat._1), ALWAYAHAS, 0)
          //一直有日志
          case ALWAYAHAS => AssetLogStats += stat._1 -> (countRdd(stat._1), ALWAYAHAS, 0)
        }
      } else {
        //没有日志的情况
        stat._2._2 match {
          //新添加的资产 第一有数据
          case NEWMEMB => {
            val msg = "告警"
            alarmMsgHandler(msg, stat._1)
            AssetLogStats += stat._1 -> (stat._2._1, HAS2NONE, 0)
          }
          //上次没有日志 这次也没有
          case HAS2NONE =>
            AssetLogStats += stat._1 -> (stat._2._1, ALWAYSNONE, 1)
          //一直没有日志  这次也没有 次数满5次 发告警
          case ALWAYSNONE if stat._2._3 % 5 == 0 => {
            val msg = "告警"
            alarmMsgHandler(msg, stat._1)
            AssetLogStats += stat._1 -> (stat._2._1, HAS2NONE, 0)
          }
          //一直没有日志  这次也没有 次数不满5次 不发告警
          case ALWAYSNONE if stat._2._3 % 5 != 0 => AssetLogStats += stat._1 -> (stat._2._1, stat._2._2, stat._2._3 + 1)
          //上次有日志
          case NONE2HAS => {
            val msg = "告警"
            alarmMsgHandler(msg, stat._1)
            AssetLogStats += stat._1 -> (stat._2._1, HAS2NONE, 0)

          }
          //一直有日志
          case ALWAYAHAS => {
            val msg = "告警"
            alarmMsgHandler(msg, stat._1)
            AssetLogStats += stat._1 -> (stat._2._1, HAS2NONE, 0)
          }
        }
      }

    })
  }
}
