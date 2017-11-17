package cn.lsmsp.sparkframe.dbopt.utils

import java.sql.{Connection, ResultSet}

object SqlUtils {
def query(conn:Connection, sql:String): ResultSet ={
  val statement = conn.prepareStatement(sql)
   statement.executeQuery()
}
}
