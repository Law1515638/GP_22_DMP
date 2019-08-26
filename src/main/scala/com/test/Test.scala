package com.test

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 聚合
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object Test {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("参数错误")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName("test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // 获取数据
    val jsonStrRDD: RDD[String] = sc.textFile(inputPath)

    // 通过JSON工具得到Businessarea 字段集合
    val res1: Map[String, Int] = jsonStrRDD.map(str => {
      TestUtils.getbusinessarea(str).groupBy(x => x).mapValues(_.size).map(x => (x._1._1, x._2))
    })
      // 合并集合中的map，将相同key 的value 相加
      .reduce((map1, map2) => {
        (map1 /: map2) ((map, kv) => {
          map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
        })
      })

    // 通过JSON工具得到Type 字段集合
    val res2: Map[String, Int] = jsonStrRDD.map(x => {
      TestUtils.getType(x)
        .map(str => {
          // 设置标签
          TagsType.makeTag(str)
        })
        .reduce((list1, list2) => list1 ::: list2).groupBy(x => x).mapValues(_.size).map(x => (x._1._1, x._2))
    })
      // 合并集合中的map，将相同key 的value 相加
      .reduce((map1, map2) =>{
        (map1 /: map2)((map, kv) => {
          map + (kv._1 -> (kv._2 + map.getOrElse(kv._1, 0)))
        })
      })

    println(res1)
    println(res2)
    sc.stop()
  }
}
