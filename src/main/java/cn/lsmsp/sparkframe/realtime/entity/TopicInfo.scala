package cn.lsmsp.sparkframe.realtime.entity

import kafka.common.TopicAndPartition

/**
  * Created by wangcongjun on 2017/4/17.
  */
/**
  * topic  信息
  * @param tName topic的名字
  * @param parOffs 每个partition对应的offsset值
  */
case class TopicInfo(tName:String,parOffs:Map[Int,Long]) {

def getTP():Map[TopicAndPartition,Long]={
  parOffs.map(x=>(new TopicAndPartition(tName,x._1),x._2))
}

}
