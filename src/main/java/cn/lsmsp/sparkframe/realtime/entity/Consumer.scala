package cn.lsmsp.sparkframe.realtime.entity

/**
  * Created by wangcongjun on 2017/4/17.
  */
/**
  * 消费者信息
  * @param brokers 消费的kafka集群信息
  * @param groupId 所属组
  * @param topicInfos 消费的topics信息
  */
case class Consumer(brokers:String,groupId:String,topicInfos:List[TopicInfo]) {
def getKafkaInfo():Map[String,String]={
    Map("metadata.broker.list"->brokers,"group.id"->groupId)
  }
}
