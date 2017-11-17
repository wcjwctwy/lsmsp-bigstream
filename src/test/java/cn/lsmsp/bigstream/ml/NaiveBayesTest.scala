package cn.lsmsp.bigstream.ml

import scala.collection.mutable.{ListBuffer, Map}

object NaiveBayesTest {
  def main (args: Array[String]): Unit = {
    val map = Map[String,ListBuffer[(String,Int)]]()

   var tmp = ListBuffer[(String,Int)]()
    map.+=("x"->tmp)
    tmp += (("",4))
    println(tmp)
    tmp = tmp.filter(_._2 > 0).map(x=>(x._1,x._2+2))
    println(map)
  }
}

