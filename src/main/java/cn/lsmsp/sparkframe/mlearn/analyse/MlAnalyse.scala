package cn.lsmsp.sparkframe.mlearn.analyse

import java.io.File

import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._

object MlAnalyse {
  /**
    * 对指定的日志进行分词
    * @param str 日志内容
    * @return
    */
  def analyse(str: String): Seq[String] = {
    //只关注这些词性的词
    val expectedNature = Array[String]("n", "v", "vd", "vn", "vf", "vx", "vi", "vl", "vg", "nt", "nz", "nw", "nl", "ng", "userDefine", "wh", "en")
    //    val str = FileUtils.readFileToString(f)
    val term = ToAnalysis.parse(str)
    term.asScala.filter(t => {
      expectedNature.contains(t.getNatureStr)
    }).map(_.getName).toSeq
  }

  /**
    * 对所有的训练文本中进行分词
    * @param trainFile 训练文本
    * @return
    */
  def analyse(trainFile:File): Seq[String] = {
    //只关注这些词性的词
    val expectedNature = Array[String]("n", "v", "vd", "vn", "vf", "vx", "vi", "vl", "vg", "nt", "nz", "nw", "nl", "ng", "userDefine", "wh", "en")
    val str = FileUtils.readFileToString(trainFile)
    val term = ToAnalysis.parse(str)
    term.asScala.filter(t => {
      expectedNature.contains(t.getNatureStr)
    }).map(_.getName).toSeq
  }
}
