package com.test

/**
  * Description: Type 加标签
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object TagsType {

  def makeTag(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    // 获取数据
    val str = args(0).asInstanceOf[String]
    // 切分
    val words: Array[String] = str.split(";")
    // 加标签
    words.foreach(word => {
      list :+= ("T" + word, 1)
    })

    list
  }
}
