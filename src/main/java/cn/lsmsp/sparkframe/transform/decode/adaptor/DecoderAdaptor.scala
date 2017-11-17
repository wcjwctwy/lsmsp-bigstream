package cn.lsmsp.sparkframe.transform.decode.adaptor

import cn.lsmsp.sparkframe.common.log.Logging

/**
  * Created by wangcongjun on 2017/7/6.
  */
trait DecoderAdaptor[T] extends Logging with Serializable{
  def adaptorDecoder(array: Array[Byte]):T
}
