package com.ProCityCt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Description: 需求指标二 统计各省市 分布情况
  *
  * @author Law
  * @version 1.0, 2019/8/21
  */
object proCity {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("目录参数不正确")
      sys.exit()
    }
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式，采用Kryo 序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    // 设置压缩格式，使用Snappy 方式进行压缩
    sQLContext.setConf("spark.sql.parquet.compression.codec", "snappy")
    // 读取本地文件
    val df: DataFrame = sQLContext.read.parquet(inputPath)
    // 注册临时表
    df.registerTempTable("t_procity")
    // 指标统计
    val res = sQLContext.sql("select count(1) ct, provincename, cityname from t_procity group by provincename, cityname")
    // 存储数据，减少分区以减少结果文件
//    res.coalesce(1).write.partitionBy("provincename", "cityname").json("hdfs://hadoop01:9000/gp22/out-20190821-1")

    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", load.getString("jdbc.user"))
    prop.setProperty("password", load.getString("jdbc.password"))
    res.write.mode("append").jdbc(load.getString("jdbc.url"), load.getString("jdbc.TableName"), prop)
    sc.stop()
  }
}
