package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.{HBaseUtils, JedisConnectionPool, TagUtils}
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 上下文标签
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsContext3 {
  def main(args: Array[String]): Unit = {
    if(args.length != 4){
      println("目录不匹配，退出程序")
      sys.exit()
    }
    val Array(inputPath, outputPath, stopPath, days)=args
    // 创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // todo 调用HBase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop 人物
    val configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))
    configuration.set("hbase.zookeeper.property.clientPort", load.getString("hbase.port"))
    // 创建HBaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    if (!hbadmin.tableExists(TableName.valueOf(hbaseTableName))) {
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)
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
        val business = TagsBusiness.makeTags(row)
        (userId, adList ++ appList ++ canalList ++ equipmentList ++ keywordList ++ locationList ++ business)
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
      ).map{
      case(userid, userTag)=>{
        val put = new Put(Bytes.toBytes(userid))
        // 处理标签
        val tags = userTag.map(t => t._1 + "," + t._2).mkString(";")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$days"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)
    sc.stop()
  }
}
