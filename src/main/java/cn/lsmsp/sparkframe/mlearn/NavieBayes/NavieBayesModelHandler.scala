package cn.lsmsp.sparkframe.mlearn.NavieBayes

import cn.lsmsp.sparkframe.mlearn.analyse.MlAnalyse
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint

import scala.collection.mutable.ArrayBuffer

object NavieBayesModelHandler {
  /**
    * 将文本转换成Vector
    * @param text 转换文本
    * @param seqAll 所有特征集
    * @return
    */
  def transText2Vector(text:String,seqAll:Seq[String]): Vector ={
    val indices = ArrayBuffer[Int]()
    val values = ArrayBuffer[Double]()
    MlAnalyse.analyse(text).foreach(word => {
      //获取特征word在seqAll中的下标
      val index = seqAll.indexOf(word)
      indices += index
      values += 1.0
    })
    //创建矢量Vector
    Vectors.sparse(seqAll.size, indices.toArray, values.toArray)
  }

  /**
    * 直接通过训练集获取Model
    * @param sc SparkContext
    * @param trainTextFileName 训练集文件路径及名称
    * @param seqAll 所有日志特征集合
    * @return 贝叶斯训练模型
    */
  def getModel(sc:SparkContext,trainTextFileName:String,seqAll:Seq[String]): NaiveBayesModel ={
    val lines = sc.textFile(trainTextFileName)
    val parsedDataRDD = lines.map(line => {
      val _line = line.split("::>")
      val lable = _line(0)
      val data = _line(1)
      //创建矢量Vector
      val vector = transText2Vector(data,seqAll)
      //创建LabeledPoint
      LabeledPoint(lable.toDouble, vector)
    })
    // 建立贝叶斯分类模型并训练
    NaiveBayes.train(parsedDataRDD, lambda = 1.0, modelType = "multinomial")
  }

  /**
    * 测试Model的准确性，开发Model时用
    * @param sc SparkContext
    * @param trainTextFileName 训练集文件路径及名称
    * @param seqAll 所有日志特征集合
    */
  def testModel(sc:SparkContext,trainTextFileName:String,seqAll:Seq[String]): Unit ={
    val lines = sc.textFile(trainTextFileName)
    val parsedDataRDD = lines.map(line => {
      val _line = line.split("::>")
      val lable = _line(0)
      val data = _line(1)
      //创建矢量Vector
      val vector = transText2Vector(data,seqAll)
      //创建LabeledPoint
      LabeledPoint(lable.toDouble, vector)
    })
    // 样本数据划分,训练样本占0.8,测试样本占0.2
    val dataParts = parsedDataRDD.randomSplit(Array(0.7, 0.3))
    val trainRDD = dataParts(0)
    val testRDD = dataParts(1)
    // 建立贝叶斯分类模型并训练
    val model = NaiveBayes.train(trainRDD, lambda = 1.0, modelType = "multinomial")
    //    model.save(sc,"/temp/model")

    // 对测试样本进行测试
    val predictionAndLabel = testRDD.map(p => (model.predict(p.features), p.label, p.features))
    val showPredict = predictionAndLabel.take(50)
    println("Prediction" + "\t" + "Label" + "\t" + "Data")
    for (i <- 0 to showPredict.length - 1) {
      println(showPredict(i)._1 + "\t" + showPredict(i)._2 + "\t" + showPredict(i)._3)
    }
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / testRDD.count()
    println("Accuracy=" + accuracy)
  }
}
