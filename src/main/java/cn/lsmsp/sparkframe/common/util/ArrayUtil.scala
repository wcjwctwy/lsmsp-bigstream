package cn.lsmsp.sparkframe.common.util

import java.util

import scala.collection.mutable.ArrayBuffer

/**
  * 操作数组的公共工具类
  */
object ArrayUtil {
  /**
   * 弹出第一个元素
    * 取出第一个元素返回 原集合中删除次元素
   */
  def pop[T](arr: ArrayBuffer[T]): T = {
    val _value = arr(0)
    arr.remove(0)
    _value
  }

  /**
    *
    * 先对简单的排列组合进行计算  最简单的就是一个元素
    * 添加一个元素后 ，先复制之前的排列（src）情况 生成新的数组 长度为src*2+1
    * (新元素先和之前的元素组合，在把自己加进去，之前的排列组合不能丢)
    *
    * @param src 需要排列组合的数组
    * @param arr 排列组合的中间结果 初始为空
    * @param offset 新元素的位置
    * @return
    */

  def combina(src: Array[String], arr: Array[String], offset: Int): Array[String] = {
    val length: Int = src.length
    if (length == offset) arr
    else { //复制一个新的数组 长度是原数组长度的2倍+1
      val newLen: Int = arr.length * 2 + 1
      val tar: String = src(offset)
      val newArr: Array[String] = util.Arrays.copyOf(arr, newLen)
      //遍历原数组 按照指定的规则将原数组元素和新元素追加进新数组
      var i: Int = 0
      while (i < arr.length) {
        newArr(arr.length + i) = arr(i) + ";" + tar
          i += 1
      }
      //将新元素单独加入新数组
      newArr(newLen - 1) = tar
      combina(src, newArr, offset + 1)
    }
  }

  def main(args: Array[String]): Unit = {
    val strings = combina(Array[String]("aa", "bb", "cc", "dd", "kk", "ee", "ff", "gg", "hh", "ii"), new Array[String](0), 0)
    print(strings.size)
    print(strings.toBuffer)
  }

}