package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description: 地域标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsLocation extends Tag{
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取省、市
    val province = row.getAs[String]("provincename")
    if (StringUtils.isNotBlank(province))
      list :+= ("ZP" + province, 1)
    val city = row.getAs[String]("cityname")
    if (StringUtils.isNotBlank(city))
      list :+= ("ZC" + city, 1)

    list
  }
}
