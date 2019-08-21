package com.util

/**
  * Description: 数据类型转换
  *
  * @author Law
  * @version 1.0, 2019/8/20
  */
object Utils2Type {

  // String 转 Int
  def toInt(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _ : Exception => 0
    }
  }

  // String 转 Double
  def toDouble(str: String): Double = {
    try {
      str.toDouble
    } catch {
      case _ : Exception => 0.0
    }
  }
}
