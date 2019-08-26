package com.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 将字典文件数据，存储到redis中
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object App2Jedis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    // 获取app 字典文件字段
    val map = sc.textFile("files/app_dict.txt")
      .map(_.split("\\s"))
      .filter(_.length >= 5)
      .mapPartitions(arr => {
        arr.map(arr => {
          (arr(4), arr(1))
        })
      }).collect.toMap
    JedisConnectionPool.hset("app_dict", map)
    sc.stop()
  }
}
