package com.Tags

import com.util.JedisConnectionPool
import redis.clients.jedis.Jedis

/**
  * Description: XXX
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object Test {
  def main(args: Array[String]): Unit = {

    val jedis = JedisConnectionPool.getConnection()

    val str: String = jedis.get("aaa")

    println(str)


  }

}
