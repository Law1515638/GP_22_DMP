package com.Tags

import com.util.Tag
import org.apache.spark.sql.Row

/**
  * Description: 渠道标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsCanal extends Tag{
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取渠道Id
    val canalId = row.getAs[Int]("adplatformproviderid")
    list :+= ("CN" + canalId, 1)


    list
  }
}
