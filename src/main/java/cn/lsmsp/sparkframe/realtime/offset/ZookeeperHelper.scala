package cn.lsmsp.sparkframe.realtime.offset

import org.I0Itec.zkclient.ZkClient

class ZookeeperHelper(servers: String) extends Serializable {
  
  @transient var zKClient_ : ZkClient = null
  private val OFFSET = -1L //offset是否自定义

  //生成zk连接
  def zkClient: ZkClient = {
    if (zKClient_ == null) {
      zKClient_ = new ZkClient(servers)
    }
    zKClient_
  }

  def getTaskNames: String ={
    ""
  }

}

//获取当前运行的任务名称

object ZookeeperHelper {
  implicit def helper2Client(zookeeperHelper: ZookeeperHelper): ZkClient = {
    zookeeperHelper.zkClient
  }
  def apply(servers: String): ZookeeperHelper = {
    new ZookeeperHelper(servers)
  }
}