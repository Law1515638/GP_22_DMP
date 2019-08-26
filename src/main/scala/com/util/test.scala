package com.util

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 测试
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List("116.310003,39.991957")
    val rdd = sc.makeRDD(list)
    val bs = rdd.map(t => {
      val arr = t.split(",")
      AmapUtils.getBusinessFromAmap(arr(0).toDouble, arr(1).toDouble)
    })
    bs.foreach(println)
  }
}
