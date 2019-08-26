package com.Tags

import com.util.{HBaseUtils, JedisConnectionPool, TagUtils}
import org.apache.hadoop.hbase.TableName
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 上下文标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsContext2 {
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
    df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .mapPartitions(row => {
      val jedis = JedisConnectionPool.getConnection()
      val res: Iterator[(String, List[(String, Int)])] = row.map(row => {
        // 去除用户Id
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row 数据 打上所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        val appList = TagsApp.makeTags(row, jedis)
        val canalList = TagsCanal.makeTags(row)
        val equipmentList = TagsEquipment.makeTags(row)
        val keywordList = TagsKeyWords.makeTags(row, bcstopword)
        val locationList = TagsLocation.makeTags(row)
        (userId, adList ++ appList ++ canalList ++ equipmentList ++ keywordList ++ locationList)
      })
      jedis.close()
      res
    })
      .reduceByKey((list1, list2) =>
        // List((String, Int))
        (list1 ::: list2)
          .groupBy(_._1)
          .mapValues(_.foldLeft[Int](0)(_ + _._2))
          .toList
      ).foreachPartition(iter => {
      val connection = HBaseUtils.getConnection()
      val tableName = TableName.valueOf("gp_22:graph")
      iter.foreach(tup => HBaseUtils.setData(connection, tableName, "tags", tup))
    })
    sc.stop()
  }
}
