import java.io.File
import java.util
import java.util.{ArrayList, Collection, UUID}

import org.apache.commons.io.FileUtils
import org.apache.solr.client.solrj.SolrQuery
import org.apache.solr.client.solrj.beans.Field
import org.apache.solr.client.solrj.impl.{CloudSolrServer, HttpSolrServer}
import org.apache.solr.common.{SolrDocument, SolrInputDocument}
import org.apache.solr.common.params.SolrParams

import scala.collection.JavaConverters._
import scala.annotation.meta.field

class SolrTest {

}

//注册model,时间类型可以为字符串，只要后台索引配置为Long即可，注解映射形式如下
case class Record(
                   @(Field@field)("rowkey") rowkey: String,
                   @(Field@field)("title") title: String,
                   @(Field@field)("content") content: String,
                   @(Field@field)("isdel") isdel: String,
                   @(Field@field)("t1") t1: String,
                   @(Field@field)("t2") t2: String,
                   @(Field@field)("t3") t3: String,
                   @(Field@field)("id") id: String,
                   @(Field@field)("dtime") dtime: String


                 )

/** *
  * Spark构建索引==>Solr
  */
object SparkIndex {

  //solr客户端
  val client = new HttpSolrServer("http://devdn01:8983/solr/spark_test");
  //批提交的条数
  val batchCount = 10000;
  val hss = new CloudSolrServer("10.20.4.160:2181,10.20.4.161:2181,10.20.4.162:2181/solr")
  hss.setDefaultCollection("spark_test")

  def addIndex(): Unit = {
    val d1 = Record("row1", "title", "content", "1", "01", "57", "58", UUID.randomUUID().toString, "3")
    val d2 = Record("row2", "title", "content", "1", "01", "57", "58", UUID.randomUUID().toString, "45")
    val d3 = Record("row3", "title", "content", "1", "01", "57", "58", UUID.randomUUID().toString, null)
    hss.addBean(d1)
    hss.addBean(d2)
    hss.addBean(d3)
    hss.commit(false, false)
    println("提交成功！")
    val docs: util.Collection[SolrInputDocument] = new util.ArrayList[SolrInputDocument]

    val doc: SolrInputDocument = new SolrInputDocument
    //在这里请注意date的格式，要进行适当的转化，上文已提到
  }

  /**
    * 获取日志 Authentication  Access
    * @param args
    */
  def main(args: Array[String]) {
    val classes = Array[String]("Authentication","Access")
    val cate = "Authentication";
    val hss = new CloudSolrServer("10.20.4.171:2181,10.20.4.172:2181,10.20.4.173:2181/solr")
    hss.setDefaultCollection("eventlog-collection")
    val query = new SolrQuery()
    query.setQuery(cate)
    query.set("df", "eventcategory")
    query.setStart(45000)
    query.setRows(100)
    val response = hss.query(query)
    val results = response.getResults
    print("结果总记录数：" + results.getNumFound)
    results.asScala.foreach(sd => {
      val rawevent = sd.getFieldValue("rawevent").toString
      //存入文件中
//      FileUtils.write(new File("D:\\temp\\data\\train.log"),classes.indexOf(cate)+"::>>" + rawevent+"\n","UTF-8", true)
      FileUtils.write(new File("D:\\temp\\data\\waitclass.log"),classes.indexOf(cate)+"::>>" + rawevent+"\n","UTF-8", true)
    })

  }
}
