package cn.lsmsp.sparkframe.transform

import scala.collection.mutable
import kafka.serializer.Decoder
import java.util.Properties
import java.io.{BufferedReader, File, InputStreamReader}

import cn.lsmsp.sparkframe.transform.decode.EventDecoder
import cn.lsmsp.sparkframe.transform.decode.adaptor.DecoderAdaptor
import org.apache.commons.io.FileUtils


/**
  * Created by wangcongjun on 2017/4/17.
  */
object DataUtils {
  var _prop:Properties = _
  val decoders = mutable.Map[String, DecoderAdaptor[Map[String,Any]]]()
  val PACKAGENAME = "cn.lsmsp.sparkframe.transform.decode."

  def getDecoder(calssName: String): DecoderAdaptor[Map[String,Any]] = {
    val _calssName = PACKAGENAME + calssName
    if (decoders.get(_calssName).isDefined) {
      decoders(_calssName)
    } else {
      val decoder = Class.forName(_calssName).newInstance().asInstanceOf[DecoderAdaptor[Map[String,Any]]]
      decoders.put(_calssName, decoder)
      decoder
    }
  }

  def getDecoders(data:Map[String,String]): Map[String, Decoder[Any]] = {
    data.map(x => {
      val topic = x._1
      val className = x._2
      val decoder= topic match {
        case "lsmsp-E2C-Gather-event"=>
          {
            //获取adaptor解码类
            val value = Class.forName(PACKAGENAME+"adaptor."+className).newInstance().asInstanceOf[DecoderAdaptor[Map[String,String]]]
            new EventDecoder(value).asInstanceOf[Decoder[Any]]
          }
          //获取正常的解码类
        case _=>Class.forName(PACKAGENAME+className).newInstance().asInstanceOf[Decoder[Any]]
      }
      (topic, decoder)
    })
  }

  def readPropFile(filePath: String): Properties = {
    if(_prop==null) {
      _prop = new Properties()
      var in = this.getClass.getResourceAsStream("/" + filePath)
      if(in==null){
        in = FileUtils.openInputStream(new File("/home/bigdata/spark/realtime/conf/"+filePath))
      }
      val bf = new BufferedReader(new InputStreamReader(in))
      _prop.load(bf);
    }
    _prop
  }
}
