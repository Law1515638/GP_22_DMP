package com.ETL

import java.util.Properties

import com.util.DataFormatUtils
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Description: 数据清洗
  *
  * @author Law
  * @version 1.0, 2019/8/20
  */
object txt2Parquet {
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
    // 格式化数据
    val df: DataFrame = DataFormatUtils.DataFormat(sc, sQLContext, inputPath)
    // 统计各省市数据分布
//    provinceAndCityDistribute(df, sQLContext, sc, outputPath)
    df.write.parquet(outputPath)

    sc.stop()
  }

  // 获取数据库连接参数
  def getProperties(): (Properties, String) = {
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "root")
    val url = "jdbc:mysql://hadoop01:3306/gp22?useUnicode=true&characterEncoding=utf-8"
    (prop, url)
  }

  // 统计各省市数据量分布情况
  def provinceAndCityDistribute(df: DataFrame, sQLContext: SQLContext, sc: SparkContext, outputPath: String): Unit ={
    // 获取数据库连接
    val properties = getProperties()
    df.registerTempTable("t_invalid")
    val provinceAndCityDistribute = sQLContext.sql("select count(1) ct, provincename, cityname from t_invalid group by provincename, cityname")
    //    provinceAndCityDistribute.show()
    // 将统计数据存入mysql
    provinceAndCityDistribute.write.jdbc(properties._2, "provinceAndCityDistribute", properties._1)
    // 将结果存入HDFS
    provinceAndCityDistribute.write.partitionBy("provincename", "cityname").json(outputPath)

  }
}