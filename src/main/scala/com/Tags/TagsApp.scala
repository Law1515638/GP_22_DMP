package com.Tags

import com.util.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * Description: XXX
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsApp extends Tag{
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    val jedis = args(1).asInstanceOf[Jedis]
    // 获取app Id 和名称
    val appid = row.getAs[String]("appid")
    val appname = row.getAs[String]("appname")
    if (StringUtils.isNotBlank(appname))
      list :+= ("APP" + appname, 1)
    else if(StringUtils.isNotBlank(appid)) {
      val newName = jedis.hget("app_dict", appid)
      if (StringUtils.isNotBlank(newName))
        list :+= ("APP" + newName, 1)
      else
        list :+= ("APP其他", 1)
    }
    list
  }
}
