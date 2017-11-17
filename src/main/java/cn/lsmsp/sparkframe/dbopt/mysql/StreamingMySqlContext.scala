package cn.lsmsp.sparkframe.dbopt.mysql

import java.sql.Connection

import cn.lsmsp.sparkframe.common.util.FileUtils
import org.apache.commons.dbcp.BasicDataSourceFactory

object StreamingMySqlContext {
  def getConnection: Connection = {
    val conf = FileUtils.readJsonFile2Prop("dbconfig","streaming")
    val dataSource = BasicDataSourceFactory.createDataSource(conf)
    dataSource.getConnection
  }

  def main(args: Array[String]): Unit = {
    println(getConnection)
  }
}
