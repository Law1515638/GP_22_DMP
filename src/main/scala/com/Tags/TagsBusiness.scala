package com.Tags

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtils, JedisConnectionPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * Description: 商圈标签
  *
  * @author Law
  * @version 1.0, 2019/8/26
  */
object TagsBusiness extends Tag {
  /**
    * 打标签的方法
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度，过滤
    val long = row.getAs[String]("long")
    val lat = row.getAs[String]("lat")
    if(Utils2Type.toDouble(long) >= 73 && Utils2Type.toDouble(long) <= 135
    && Utils2Type.toDouble(lat) >= 3 && Utils2Type.toDouble(lat) <= 54) {
      // 先去数据库获取商圈
      val business = getBusiness(long.toDouble, lat.toDouble)
      // 判断商圈是否为空
      if(StringUtils.isNotBlank(business)){
        val lines = business.split(",")
        lines.foreach(f => list:+=(f, 1))
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double): String = {
    // 转换GeoHash 字符串
    val geohash = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)
    // 去数据库查询
    var business = redis_queryBusiness(geohash)
    // 判断商圈是否为空
    if(business == null || business.length == 0){
      // 通过经纬度获取商圈
      business = AmapUtils.getBusinessFromAmap(long.toDouble, lat.toDouble)
      // 如果调用高德地图解析商圈，需要将此次商圈存入redis
      redis_insertBusiness(geohash, business)
    }
    business
  }

  /**
    * 获取商圈信息
    */
  def redis_queryBusiness(geoHash: String): String = {
    val jedis = JedisConnectionPool.getConnection()
    val business = jedis.get(geoHash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */
  def redis_insertBusiness(geoHash: String, business: String): Unit = {
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geoHash, business)
    jedis.close()
  }
}
