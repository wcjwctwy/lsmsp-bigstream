package cn.lsmsp.sparkframe.realtime.adaptor

import java.util.UUID

import org.apache.spark.Partitioner

/**
  * Created by wangcongjun on 2017/7/18.
  */
class RollPartition(numPartition:Int) extends Partitioner{
  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    val uUID = UUID.randomUUID()
    val p = uUID.hashCode()%numPartitions
    if(p<0){
      numPartition+p
    }else{
      p
    }
  }
}
