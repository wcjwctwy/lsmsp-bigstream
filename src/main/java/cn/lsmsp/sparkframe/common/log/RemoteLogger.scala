package cn.lsmsp.sparkframe.common.log

import java.net.InetAddress
import java.lang.management.ManagementFactory
import cn.lsmsp.sparkframe.common.util.HttpRequest
import org.slf4j.{Logger, Marker}

class RemoteLogger(url:String) extends Logger with Serializable{
  override def getName: String = {
    ""
  }

  override def debug(s: String): Unit = {}

  override def debug(s: String, o: scala.Any): Unit = {}

  override def debug(s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def debug(s: String, objects: AnyRef*): Unit = {}

  override def debug(s: String, throwable: Throwable): Unit = {}

  override def debug(marker: Marker, s: String): Unit = {}

  override def debug(marker: Marker, s: String, o: scala.Any): Unit = {}

  override def debug(marker: Marker, s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def debug(marker: Marker, s: String, objects: AnyRef*): Unit = {}

  override def debug(marker: Marker, s: String, throwable: Throwable): Unit = {}

  override def isWarnEnabled: Boolean = {true}

  override def isWarnEnabled(marker: Marker): Boolean = {true}

  override def error(s: String): Unit = {}

  override def error(s: String, o: scala.Any): Unit = {}

  override def error(s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def error(s: String, objects: AnyRef*): Unit = {}

  override def error(s: String, throwable: Throwable): Unit = {}

  override def error(marker: Marker, s: String): Unit = {}

  override def error(marker: Marker, s: String, o: scala.Any): Unit = {}

  override def error(marker: Marker, s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def error(marker: Marker, s: String, objects: AnyRef*): Unit = {}

  override def error(marker: Marker, s: String, throwable: Throwable): Unit = {}

  override def warn(s: String): Unit = {}

  override def warn(s: String, o: scala.Any): Unit = {}

  override def warn(s: String, objects: AnyRef*): Unit = {}

  override def warn(s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def warn(s: String, throwable: Throwable): Unit = {}

  override def warn(marker: Marker, s: String): Unit = {}

  override def warn(marker: Marker, s: String, o: scala.Any): Unit = {}

  override def warn(marker: Marker, s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def warn(marker: Marker, s: String, objects: AnyRef*): Unit = {}

  override def warn(marker: Marker, s: String, throwable: Throwable): Unit = {}

  override def trace(s: String): Unit = {}

  override def trace(s: String, o: scala.Any): Unit = {}

  override def trace(s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def trace(s: String, objects: AnyRef*): Unit = {}

  override def trace(s: String, throwable: Throwable): Unit = {}

  override def trace(marker: Marker, s: String): Unit = {}

  override def trace(marker: Marker, s: String, o: scala.Any): Unit = {}

  override def trace(marker: Marker, s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def trace(marker: Marker, s: String, objects: AnyRef*): Unit = {}

  override def trace(marker: Marker, s: String, throwable: Throwable): Unit = {}

  override def isInfoEnabled: Boolean = {true}

  override def isInfoEnabled(marker: Marker): Boolean = {true}

  override def isErrorEnabled: Boolean = {true}

  override def isErrorEnabled(marker: Marker): Boolean = {true}

  override def isTraceEnabled: Boolean = {true}

  override def isTraceEnabled(marker: Marker): Boolean = {true}

  override def isDebugEnabled: Boolean = {true}

  override def isDebugEnabled(marker: Marker): Boolean = {true}

  override def info(s: String): Unit = {
    val runtime = ManagementFactory.getRuntimeMXBean
    val addr = InetAddress.getLocalHost
    val hostName = addr.getHostName
    val ip = addr.getHostAddress
    val hash = this.hashCode()
    val className = this.getClass.getName.stripSuffix("$")
    s.replaceAll("=","-").replaceAll(" ","-")
    val msg = s"logFile=$hostName&msg={ip:$ip,jvmName:${runtime.getName},log:$s}"
    HttpRequest.sendPost(url,msg)
  }

  override def info(s: String, url: scala.Any): Unit = {

  }

  override def info(s: String, url: scala.Any, logFile: scala.Any): Unit = {

  }


  override def info(s: String, objects: AnyRef*): Unit = {}

  override def info(s: String, throwable: Throwable): Unit = {}

  override def info(marker: Marker, s: String): Unit = {}

  override def info(marker: Marker, s: String, o: scala.Any): Unit = {}

  override def info(marker: Marker, s: String, o: scala.Any, o1: scala.Any): Unit = {}

  override def info(marker: Marker, s: String, objects: AnyRef*): Unit = {}

  override def info(marker: Marker, s: String, throwable: Throwable): Unit = {}

}
