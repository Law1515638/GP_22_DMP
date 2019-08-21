package com.util

import org.apache.spark.sql.Row

/**
  * Description: 指标方法
  *
  * @author Law
  * @version 1.0, 2019/8/21
  */
object RptUtils {

  // 此方法处理请求数
  def request(requestmode: Int, processnode: Int): List[Double] = {
    if (requestmode == 1 && processnode >= 1)
      List(1, 0, 0)
    else if(requestmode == 1 && processnode >= 2) List(1, 1, 0)
    else if(requestmode == 1 && processnode == 3) List(1, 1, 1)
    else
      List(0, 0, 0)
  }

  // 此方法处理展示点击数
  def click(requestmode: Int, iseffective: Int): List[Double] = {
    if (requestmode == 2 && iseffective == 1) List(1, 0)
    else if (requestmode == 3 && iseffective == 1) List(0, 1)
    else  List(0, 0)
  }

  // 此方法处理竞价操作
  def Ad(iseffective: Int, isbilling: Int, isbid: Int, iswin: Int,
         adorderid: Int, winprice: Double, adpayment: Double): List[Double] = {
    if (iseffective == 1 && isbilling == 1 && isbid == 1) {
      if (iswin == 1 && adorderid != 0)
        List[Double](1, 1, winprice/1000.0, adpayment/1000.0)
      else
        List(1, 0, 0, 0)
    } else
      List(0, 0, 0, 0)
  }

  def getList(row: Row): List[Double] = {
    val requestmode = row.getAs[Int]("requestmode")
    val processnode = row.getAs[Int]("processnode")
    val iseffective = row.getAs[Int]("iseffective")
    val isbilling = row.getAs[Int]("isbilling")
    val isbid = row.getAs[Int]("isbid")
    val iswin = row.getAs[Int]("iswin")
    val adorderid = row.getAs[Int]("adorderid")
    val winprice = row.getAs[Double]("winprice")
    val adpayment = row.getAs[Double]("adpayment")
    // key 值是地域的省市
    val pro = row.getAs[String]("provincename")
    val city = row.getAs[String]("cityname")
    // 创建三个对应的方法处理九个指标
    val list1 = RptUtils.request(requestmode, processnode)
    val list2 = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
    val list3 = RptUtils.click(requestmode, iseffective)

    list1 ::: list2 ::: list3
  }
}
