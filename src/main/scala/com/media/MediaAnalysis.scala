package com.media

import com.util.RptUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Description: 需求指标五  统计各媒体 请求及竞标参数
  *
  * @author Law
  * @version 1.0, 2019/8/21
  */
object MediaAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      println("目录参数不正确")
      sys.exit()
    }
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式，采用Kryo 序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 清洗app 文件，获得全部id 和name ，方便后面的查询
    val appIdAndName: collection.Map[String, String] = sc.textFile("files/app_dict.txt")
      .map(_.split("\t"))
      .filter(_.length >= 5)
      .map(arr => {
        val appId = arr(4)
        val appName = arr(1)
        (appId, appName)
      }).collectAsMap()
    // 将map 广播到Executor
    val appTupBroadcast: Broadcast[collection.Map[String, String]] = sc.broadcast(appIdAndName)
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)
    val appNameTup: RDD[(String, List[Double])] = df.map(row => {
      var realName: String = ""
      val list: List[Double] = RptUtils.getList(row)
      val appId = row.getAs[String]("appid")
      val appName = row.getAs[String]("appname")
      if (appName.equals("其他"))
        realName = appTupBroadcast.value.getOrElse(appId, "其他")
      else
        realName = appName
      (realName, list)
    })
    // 聚合
    val resRDD: RDD[(String, List[Double])] = appNameTup.reduceByKey((x, y) => (x zip y).map(x => x._1 + x._2))
    // rdd -> DF 输出
    val resRow: RDD[Row] = resRDD.map(rdd => {
      val list = rdd._2
      Row(rdd._1, list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8))
    })
    val schema = StructType(StructField("媒体类别", StringType) ::
      StructField("原始请求", DoubleType) :: StructField("有效请求", DoubleType) :: StructField("广告请求数", DoubleType)
      :: StructField("参与竞价数", DoubleType) :: StructField("竞价成功数", DoubleType) :: StructField("广告消费", DoubleType) :: StructField("广告成本", DoubleType)
      :: StructField("展示数", DoubleType) :: StructField("点击数", DoubleType) :: Nil)
    val res: DataFrame = sQLContext.createDataFrame(resRow, schema)
    res.show()
    sc.stop()
  }
}
