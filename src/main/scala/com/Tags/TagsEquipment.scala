package com.Tags

import com.util.Tag
import org.apache.spark.sql.Row

/**
  * Description: 设备标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsEquipment extends Tag{
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取操作系统，联网方式，运营商
    val clientID = row.getAs[Int]("client")
    clientID match {
      case v if v == 1 => list :+= ("D00010001", 1)
      case v if v == 2 => list :+= ("D00010002", 1)
      case v if v == 3 => list :+= ("D00010003", 1)
      case _ => list :+= ("D00010004", 1)
    }
    val networkName = row.getAs[String]("networkmannername")
    networkName match {
      case v if v.equalsIgnoreCase("WIFI") => list :+= ("D00020001", 1)
      case v if v.equalsIgnoreCase("4G") => list :+= ("D00020002", 1)
      case v if v.equalsIgnoreCase("3G") => list :+= ("D00020003", 1)
      case v if v.equalsIgnoreCase("2G") => list :+= ("D00020004", 1)
      case _ => list :+= ("D00020005", 1)
    }
    val ispName = row.getAs[String]("ispname")
    ispName match {
      case v if v.equals("移动") => list :+= ("D00030001", 1)
      case v if v.equals("联通") => list :+= ("D00030002", 1)
      case v if v.equals("电信") => list :+= ("D00030003", 1)
      case _ => list :+= ("D00030004", 1)
    }
    list
  }
}
