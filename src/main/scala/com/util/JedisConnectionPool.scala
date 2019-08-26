package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * Description: Jedic 连接池
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object JedisConnectionPool {

  val config = new JedisPoolConfig()

  // 设置最大连接数
  config.setMaxTotal(20)
  // 最大空闲
  config.setMaxIdle(10)
  // 创建连接
  val pool = new JedisPool(config, "hadoop01", 6379, 10000, "root")

  def getConnection(): Jedis = {
    pool.getResource
  }

  def hset(key: String, fieldValues: Map[String, String]): Unit = {
    import scala.collection.JavaConverters._
    val data = fieldValues.map(element => {
      (element._1.getBytes(), element._2.getBytes())
    }).asJava
    val jedis = JedisConnectionPool.getConnection
    if (data.size() > 0)
      jedis.hmset(key.getBytes(), data)
    jedis.close()
  }

  def hget(key: String, field: String): String = {
    val jedis = JedisConnectionPool.getConnection
    val res = jedis.hget(key, field)
    jedis.close()
    res
  }

}
