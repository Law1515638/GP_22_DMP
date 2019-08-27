package com.Tags

import com.typesafe.config.ConfigFactory
import com.util.{JedisConnectionPool, TagUtils}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 最终项目
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object TagsContext5 {
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
    // 创建Hadoop 任务
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
    val baseRDD: RDD[(List[String], Row)] = df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .map(row => {
      val userList: List[String] = TagUtils.getAllUserId(row)
      (userList, row)
    })
    // 构建点集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val jedis = JedisConnectionPool.getConnection()
      val row = tp._2
      // 所有标签
      val adList = TagsAd.makeTags(row)
      val appList = TagsApp.makeTags(row, jedis)
      val canalList = TagsCanal.makeTags(row)
      val equipmentList = TagsEquipment.makeTags(row)
      val keywordList = TagsKeyWords.makeTags(row, bcstopword)
      val locationList = TagsLocation.makeTags(row)
      val business = TagsBusiness.makeTags(row)
      val AllTag = adList ++ appList ++ canalList ++ equipmentList ++ keywordList ++ locationList ++ business
      // List((String, Int))
      // 保证其中一个点携带着所有标签，同时也保留所有userId
      val VD = tp._1.map((_, 0)) ++ AllTag
      // 处理所有的点
      val res = tp._1.map(uId => {
        // 保证一个点携带标签
        if (tp._1.head.equals(uId)) {
          (uId.hashCode.toLong, VD)
        } else {
          (uId.hashCode.toLong, List.empty)
        }
      })
      jedis.close()
      res
    })
    // 构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    // 构建图
    val graph = Graph(vertiesRDD, edges)
    // 取出顶点
    val vertices = graph.connectedComponents().vertices
    vertices.join(vertiesRDD).map{
      case (uId, (conId, tagsAll)) => (conId, tagsAll)
    }.reduceByKey((list1, list2) => {
      (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    })
      // 输出到HBase
      .map{
      case(userid, userTag)=>
        val put = new Put(Bytes.toBytes(userid))
        // 处理标签
        val tags = userTag.map(t => t._1 + "," + t._2).mkString(";")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(s"$days"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
    }
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)

    sc.stop()
  }
}
