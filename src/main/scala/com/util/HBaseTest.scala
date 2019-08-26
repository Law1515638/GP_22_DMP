package com.util

import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Description: HBase API 测试
  *
  * @author Law
  * @version 1.0, 2019/8/25
  */
object HBaseTest {
  def main(args: Array[String]): Unit = {
  }

  def createTableTest(): Unit = {
    val connection = HBaseUtils.getConnection()
    val tableName = TableName.valueOf("gp_22:test")
    HBaseUtils.createHTable(connection, tableName, "info")
  }

  def setTest(): Unit = {
    val connection = HBaseUtils.getConnection()
    val tableName = TableName.valueOf("gp_22:test")
    val tuple = ("IM: 73555616068654",List(("LC01",1), ("LNbanner",1), ("APP资讯-and",1), ("CN100016",1), ("D00010001",1), ("D00020005",1), ("D00030004",1), ("ZP陕西省",1), ("ZC榆林市",1)))
    HBaseUtils.setData(connection, tableName, "info", tuple)
  }

  def getTest(): Unit = {

    val connection = HBaseUtils.getConnection()
    val tableName = TableName.valueOf("gp_22:graph")
    val cells = HBaseUtils.getData(connection, tableName, "IM: 47544899705513")

    for (cell <- cells) {
      println("rowKey: " + Bytes.toString(CellUtil.cloneRow(cell)) + " " +
        "Family: " + Bytes.toString(CellUtil.cloneFamily(cell)) + " " +
        "Qualifier: " + Bytes.toString(CellUtil.cloneQualifier(cell)) + " " +
        "Value: " + Bytes.toInt(CellUtil.cloneValue(cell)))
    }
  }
}
