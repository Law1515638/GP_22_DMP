package com.terminal

import com.util.RptUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  * Description: 需求指标四  统计各网络类型 请求及竞标参数
  *
  * @author Law
  * @version 1.0, 2019/8/21
  */
object NetWork {
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
    // 读取数据
    val df = sQLContext.read.parquet(inputPath)

    // SparkSQL 方法求结果
//    sparkSQL(df, sQLContext)
    // SparkCore 方法求结果
    sparkCore(df, sQLContext)

    sc.stop()
  }


  def sparkSQL(df: DataFrame, sQLContext: SQLContext): Unit = {
    // 创建临时表
    df.registerTempTable("t_invalid")
    // sql 数据查询
    val RptDF: DataFrame = sQLContext.sql("select " +
      "networkmannername, " +
      "sum(is_initial_request) ct_initial_request, " +
      "sum(is_valid_request) ct_valid_request, " +
      "sum(is_ad_request) ct_ad_request, " +
      "sum(is_join_bidding) ct_join_bidding, " +
      "sum(is_success_bidding) ct_success_bidding, " +
      "sum(is_show) ct_show, " +
      "sum(is_click) ct_click, " +
      "sum(winprice)/1000 winprice, " +
      "sum(adpayment)/1000 adpayment " +
      "from " +
      "( " +
      "select " +
      "networkmannername, " +
      "(case when requestmode=1 and processnode>=1 then 1 else 0 end) is_initial_request, " +
      "(case when requestmode=1 and processnode>=2 then 1 else 0 end) is_valid_request, " +
      "(case when requestmode=1 and processnode=3 then 1 else 0 end) is_ad_request, " +
      "(case when iseffective=1 and isbilling=1 and isbid=1 then 1 else 0 end) is_join_bidding, " +
      "(case when iseffective=1 and isbilling=1 and iswin=1 and adorderid!=0 then 1 else 0 end) is_success_bidding, " +
      "(case when requestmode=2 and iseffective=1 then 1 else 0 end) is_show, " +
      "(case when requestmode=3 and iseffective=1 then 1 else 0 end) is_click, " +
      "(case when iseffective=1 and isbilling=1 and iswin=1 then winprice else 0 end) winprice, " +
      "(case when iseffective=1 and isbilling=1 and iswin=1 then adpayment else 0 end) adpayment " +
      "from t_invalid " +
      ") t " +
      "group by networkmannername")
    RptDF.show()
  }

  def sparkCore(df: DataFrame, sQLContext: SQLContext): Unit = {
    // 将数据进行处理，统计各种指标
    val networkTup: RDD[(String, List[Double])] = df.map(row => {
      // 把需要的字段全部取到
      val list = RptUtils.getList(row)
      // key 值是网络类型
      val networkmannername = row.getAs[String]("networkmannername")
      (networkmannername, list)
    })
    // 聚合
    val RptRDD: RDD[(String, List[Double])] = networkTup.reduceByKey((x, y) => (x zip y).map(x => x._1 + x._2))

    val RptRow: RDD[Row] = RptRDD.map(rdd => {
      val list = rdd._2
      Row(rdd._1, list(0), list(1), list(2), list(3), list(4), list(5), list(6), list(7), list(8))
    })
    val schema = StructType(StructField("网络类型", StringType) ::
      StructField("原始请求", DoubleType) :: StructField("有效请求", DoubleType) :: StructField("广告请求数", DoubleType)
      :: StructField("参与竞价数", DoubleType) :: StructField("竞价成功数", DoubleType) :: StructField("广告消费", DoubleType) :: StructField("广告成本", DoubleType)
      :: StructField("展示数", DoubleType) :: StructField("点击数", DoubleType) :: Nil)
    val resDF: DataFrame = sQLContext.createDataFrame(RptRow, schema)
    resDF.show()
  }
}
