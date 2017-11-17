package cn.lsmsp.bigstream.rbiz.demo

import cn.lsmsp.sparkframe.realtime.RealtimeBusi

object DemoInvoker extends RealtimeBusi {

  def main(args: Array[String]): Unit = {
    var jobName:String="default"
    if(args.length>0){
      jobName = args(0)
    }
    start()
  }
}