package com.util

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, HColumnDescriptor, HTableDescriptor, NamespaceDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put}
import org.apache.hadoop.hbase.util.Bytes

/**
  * Description: HBase API 工具类
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object HBaseUtils {

  private[this] var connection: Connection = _

  // 创建命名空间测试
  def main(args: Array[String]): Unit = {
    val connection = HBaseUtils.getConnection()
    val admin = connection.getAdmin
    val descriptor = NamespaceDescriptor.create("gp_22").build()
    admin.createNamespace(descriptor)
  }

  // 获得HBase 连接
  def getConnection() = {
    if (connection == null) {
      val conf = HBaseConfiguration.create()
      // 配置信息
      conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      this.connection = ConnectionFactory.createConnection(conf)
    }
    connection
  }

  /**
    * HBase 建表
    * @param connection:HBase 连接
    * @param tableName: 表名称
    * @param familyName: 列族名
    */
  def createHTable(connection: Connection, tableName: TableName, familyName: String): Unit = {
    // 创建Admin
    val admin = connection.getAdmin
    // 判断表是否已存在
    if (!admin.tableExists(tableName)) {
      // 表描述器
      val tableDesc = new HTableDescriptor(tableName)
      // 列族描述器
      val columnDesc = new HColumnDescriptor(familyName)
      // 列族加入到表中
      tableDesc.addFamily(columnDesc)
      // 创建表
      admin.createTable(tableDesc)
      println("建表成功！")
    } else
      println("表已存在")
  }

  /**
    * 插入数据到HBase
    * @param connection: HBase连接
    * @param tableName: 表名称
    * @param familyName: 列族名
    * @param data: 数据
    */
  def setData(connection: Connection, tableName: TableName, familyName: String, data: (String, List[(String, Int)])): Unit = {
    // 定义表的类
    val table = connection.getTable(tableName)
    // 判断表是否不存在
    if (table != null) {
      // 定义rowkey
      val put = new Put(Bytes.toBytes(data._1))
      // 获取tup 里的Qualifier 与Value
      val list = data._2
      // 对list 循环存入
      for (elem <- list) {
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(elem._1), Bytes.toBytes(elem._2))
      }
      // 存入数据
      table.put(put)
    } else {
      println("表不存在")
    }
  }

  /**
    * 查询HBase 数据
    * @param connection: HBase连接
    * @param tableName: 表名称
    * @param rowKey: 关键字
    * @return Cells 结果集
    */
  def getData(connection: Connection, tableName: TableName, rowKey: String): Array[Cell] = {
    // 获取表实例
    val table = connection.getTable(tableName)
    // 定义rowkey
    val get = new Get(Bytes.toBytes(rowKey))
    // 定义结果集
    val result = table.get(get)
    // 定义单元格数组，并从result结果集中拿去数据转换单元格
    result.rawCells()
  }


}
