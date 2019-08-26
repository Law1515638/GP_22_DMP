package com.test

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}


/**
  * Description: 获取Json串中的数据
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object TestUtils {

  // 获取Businessarea 数据
  def getbusinessarea(jsonStr: String): collection.mutable.ListBuffer[(String, Int)] = {
    val jsonparse: JSONObject = JSON.parseObject(jsonStr)
    val status: Int = jsonparse.getIntValue("status")
    if (status == 0) return null
    val regeocode: JSONObject = jsonparse.getJSONObject("regeocode")
    if (regeocode == null || regeocode.keySet().isEmpty) return null
    val pois: JSONArray = regeocode.getJSONArray("pois")
    if (pois == null || pois.isEmpty) return null

    val buffer = collection.mutable.ListBuffer[(String, Int)]()
    for (item <- pois.toArray()) {
      item match {
        case json: JSONObject => buffer.append((json.getString("businessarea"), 1))
        case _ =>
      }
    }
    buffer
  }

  // 获取Type 数据
  def getType(jsonStr: String): collection.mutable.ListBuffer[String] = {
    val jsonparse: JSONObject = JSON.parseObject(jsonStr)
    val status: Int = jsonparse.getIntValue("status")
    if (status == 0) return null
    val regeocode: JSONObject = jsonparse.getJSONObject("regeocode")
    if (regeocode == null || regeocode.keySet().isEmpty) return null
    val pois: JSONArray = regeocode.getJSONArray("pois")
    if (pois == null || pois.isEmpty) return null
    val buffer = collection.mutable.ListBuffer[String]()
    for (item <- pois.toArray()) {
      item match {
        case json: JSONObject => buffer.append(json.getString("type"))
        case _ =>
      }
    }
    buffer
  }
}
