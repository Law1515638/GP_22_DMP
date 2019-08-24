package com.util

import redis.clients.jedis.{Jedis, JedisPool}
import redis.clients.util.Pool

/**
  * Description: Redis工具类
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
object RedisUtils {
  private[this] var jedisPool: Pool[Jedis] = new JedisPool("hadoop01", 6379)

  def hset(key: String, fieldValues: Map[String, String]): Unit = {
    import scala.collection.JavaConverters._
    val data = fieldValues.map(element => {
      (element._1.getBytes(), element._2.getBytes())
    }).asJava
    val jedis = jedisPool.getResource
    if (data.size() > 0)
      jedis.hmset(key.getBytes(), data)
    jedis.close()
  }

  def hget(key: String, field: String): String = {
    val jedis = jedisPool.getResource
    val res = jedis.hget(key, field)
    jedis.close()
    res
  }
}
