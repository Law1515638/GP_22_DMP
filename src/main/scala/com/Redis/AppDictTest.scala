package com.Redis

import com.util.RedisUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 将App 字典文件存入redis
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object AppDictTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 获取app 字典文件字段
    val map: Map[String, String] = sc.textFile("files/app_dict.txt")
      .map(_.split("\\s"))
      .filter(_.length >= 5)
      .map(arr => {
        val appId = arr(4)
        val appName = arr(1)
        (appId, appName)
      }).collect.toMap

    RedisUtils.hset("app_dict", map)

    sc.stop()
  }
}
