package com.graph

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
  * Description: XXX
  *
  * @author Law
  * @version 1.0, 2019/8/26
  */
object Graph_Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
    val sc = new SparkContext(conf)
    /*// 构造点的集合
    val vertexRDD = sc.makeRDD(Seq(
      (1L, ("詹姆斯", 35)),
      (2L, ("霍华德", 34)),
      (6L, ("杜兰特", 31)),
      (9L, ("库里", 30)),
      (133L, ("哈登", 30)),
      (138L, ("席尔瓦", 36)),
      (16L, ("法尔考", 35)),
      (44L, ("内马尔", 27)),
      (21L, ("J罗", 28)),
      (5L, ("高斯林", 60)),
      (7L, ("奥德斯基", 55)),
      (158L, ("码云", 55))
    ))
    // 构造边的集合
    val egde: RDD[Edge[Int]] = sc.makeRDD(Seq(
      Edge(1L, 133L, 0),
      Edge(2L, 133L, 0),
      Edge(6L, 133L, 0),
      Edge(9L, 133L, 0),
      Edge(6L, 138L, 0),
      Edge(16L, 138L, 0),
      Edge(44L, 138L, 0),
      Edge(21L, 138L, 0),
      Edge(5L, 158L, 0),
      Edge(7L, 158L, 0)
    ))
    // 构建图
    val graph = Graph(vertexRDD,egde)
    // 取出每个边上的最大顶点
    val vertices = graph.connectedComponents().vertices
    vertices.join(vertexRDD).map{
      case(userId,(conId,(name,age)))=>{
        (conId,List(name,age))
      }
    }.reduceByKey(_++_).foreach(println)*/

    val tup: (List[String], List[(String, Int)]) = (List("IM: sha%3Aff842f31c7f2340f41e2010a5b6293e159338930", "ADM: ff842f31c7f2340f41e2010a5b6293e159338930"),List(("LC01",1), ("LNbanner",1), ("APP养车资讯",1), ("CN100016",1), ("D00010004",1), ("D00020005",1), ("D00030004",1), ("ZP山东省",1), ("ZC烟台市",1)))
    makeGraph(sc, tup)
  }

  def makeGraph(sc: SparkContext, tup: (List[String], List[(String, Int)])) = {
    // 获取userid 和 标签 集合
    val userId = tup._1
    val head = userId.head
    val tags = tup._2
    // 建立点的集合
    var vertexSeq = Seq[(Long, List[(String, Int)])]()
    // 建立边的集合
    var edgeSeq = Seq[Edge[Int]]()
    // 添加点、边
    vertexSeq :+= (head.hashCode.toLong, userId.map((_, 0)) ++ tags)
    for (i <- 1 until userId.length) {
      vertexSeq :+= (userId(i).hashCode.toLong, List.empty)
      edgeSeq :+= Edge(head.hashCode.toLong, userId(i).hashCode.toLong, 1)
    }
    // 构造点集合
    val vertexRDD = sc.makeRDD(vertexSeq)
    // 构造边的集合
    val edgeRDD = sc.makeRDD(edgeSeq)
    val graph = Graph(vertexRDD, edgeRDD)
    // 取出每个边上的最大顶点
    val vertices = graph.connectedComponents().vertices
    vertices.join(vertexRDD).map{
      case (uId, (conId, tagsAll)) => (conId, tagsAll)
    }.reduceByKey((list1, list2) => {
      (list1 ++ list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).foreach(println)

    sc.stop()
  }
}
