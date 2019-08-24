package com.Tags

import com.util.TagUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 上下文标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 3){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath, outputPath, stopPath)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    // 获取停用词库
    val stopword = sc.textFile(stopPath).map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)
    // 获取符合Id 的数据
    val res: RDD[List[(String, Int)]] = df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row => {
      // 去除用户Id
      val userId = TagUtils.getOneUserId(row)
      // 接下来通过row 数据 打上所有标签（按照需求）
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp.makeTags(row)
      val canalList = TagsCanal.makeTags(row)
      val equipmentList = TagsEquipment.makeTags(row)
      val keywordList = TagsKeyWords.makeTags(row, bcstopword)
      val locationList = TagsLocation.makeTags(row)
      appList
    })
    for (elem <- res.collect) {
      println(elem)
    }
    sc.stop()
  }
}
