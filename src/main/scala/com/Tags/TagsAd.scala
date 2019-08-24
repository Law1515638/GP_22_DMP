package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description: 广告标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsAd extends Tag{
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型，广告类型名称
    val adType = row.getAs[Int]("adspacetype")
    adType match {
      case v if v > 9 => list :+= ("LC" + v, 1)
      case v if v <= 9 && v > 0 => list :+= ("LC0" + v, 1)
    }
    val adName = row.getAs[String]("adspacetypename")
    if (StringUtils.isNotBlank(adName)) {
      list :+= ("LN" + adName, 1)
    }
    list
  }
}
