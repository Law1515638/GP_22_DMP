package com.Tags

import com.util.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

/**
  * Description: 关键字标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsKeyWords extends Tag{
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val bcstopword = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
    // 获取关键字
    val keywords = row.getAs[String]("keywords")
    val words = keywords.split("\\|")
    words.filter(word => word.length >= 3 && word.length() <= 8 && bcstopword.value.get(word).isEmpty)
        .foreach(word => list :+= ("K" + word, 1))
    list
  }
}
