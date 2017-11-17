package cn.lsmsp.sparkframe.realtime.adaptor

import cn.lsmsp.sparkframe.realtime.InitContext
import cn.lsmsp.sparkframe.realtime.entity.AppContext
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangcongjun on 2017/4/13.
  */

object SparkContextAdaptor {

  private var sc: SparkContext = _
  private var ssc: StreamingContext = _
  var appContext: AppContext = _

  /**
    * 获取当前任务的sparkcontext
    *
    * @return
    */

  def getSC(appName: String): SparkContext = {
    if (appContext == null) {
      appContext = InitContext.getAppContext(appName)
    }
    if (Option(sc).isEmpty) {
      val conf = new SparkConf().setAppName(appName)
      val master = appContext.master
      if (master != null && master.nonEmpty) {
        if (master.startsWith("local")) {
          System.setProperty("hadoop.home.dir", "D:\\publictools\\hadoop-common-2.2.0-bin-master")
        }
        conf.setMaster(master)
      }
      sc = new SparkContext(conf)
//      sc.setLogLevel("WARN")
      //sc.setLogLevel("DEBUG")
      //sc.setLogLevel("ERROR")
      //sc.setLogLevel("INFO")
    }

    sc
  }

  /**
    * 获取当前实时流对象streamingcontext
    *
    * @return
    */
  def getSSC(appName: String): StreamingContext = {
    if (appContext == null) {
      appContext = InitContext.getAppContext(appName)
    }
    var period = appContext.period
    if (period < 50) {
      period = 50
    }
    if (Option(ssc).isEmpty) {
      ssc = new StreamingContext(getSC(appName), Milliseconds(period))
    }
    ssc
  }

}

