package cn.lsmsp.sparkframe.realtime.offset

import kafka.common.TopicAndPartition
import cn.lsmsp.sparkframe.realtime.entity.TopicInfo


/**
  * Created by wangcongjun on 2017/4/17.
  */
trait OffsetOpt extends Serializable{
  /**
    * 检查offset存储的path是否存在若不在则新建并且赋值为0L
    */
  def checkoffsetPath(groupId:String,topicInfos: List[TopicInfo]):Unit
  /**
    * 为存储offset创建node路径
    *
    * @param path 所创建的路径
    */
  def creatPath(path: String): Unit

  /**
    * 删除路径
    *
    * @param path    路径名
    * @param isEmpty 节点是否为空
    */
  def delPath(path: String, isEmpty: Boolean): Unit


  /**
    * 校验offset值是否正确
    * @param path
    * @param maxOffset
    * @return
    */
  def offsetValid(path:String,maxOffset:Long):Boolean

  /**
    * offset的优先级问题  在处理offset时有三种选择
    * 1、kafka的offset值
    * 2、zookeeper的offset值
    * 3、配置文件的offset值
    * 当zookeeper上没有值得时候，以配置文件为准 配置文件上也没有则以kafka上的offset为准
    */

  def offsetPriority(zkpath:String,topicInfos: Map[TopicAndPartition, Long],brokers:String):Map[TopicAndPartition, Long]

}
