package com.util

import com.alibaba.fastjson.{JSON, JSONObject}


/**
  * Description: 商圈解析工具
  *
  * @author Law
  * @version 1.0, 2019/8/24
  */
object AmapUtils {

  // 获取高德地图商圈信息
  def getBusinessFromAmap(long: Double, lat: Double): String = {
    // https://restapi.amap.com/v3/geocode/regeo?location=116.310003,39.991957&key=5d66db77a9e4536193150cd81f056861
    val location = long + "," + lat
    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?location=" + location + "&key=5d66db77a9e4536193150cd81f056861"
    // 调用请求
    val jsonStr = HttpUtils.get(urlStr)
    // 解析json串
    val jsonparse = JSON.parseObject(jsonStr)
    // 判断状态是否成功
    val status = jsonparse.getIntValue("status")
    if (status == 0) return ""
    // 接下来解析内部json串，判断每个key 的value 都不能为空
    val regeocedeJson = jsonparse.getJSONObject("regeocode")
    if (regeocedeJson == null || regeocedeJson.keySet().isEmpty) return ""

    val addressComponentJson = regeocedeJson.getJSONObject("addressComponent")
    if (addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
    if (businessAreasArray == null || businessAreasArray.isEmpty) return ""
    // 创建集合 保存数据
    val buffer = collection.mutable.ListBuffer[String]()
    // 循环输出
    for (item <- businessAreasArray.toArray()){
      item match {
        case json: JSONObject =>
          buffer.append(json.getString("name"))
        case _ =>
      }
    }
    buffer.mkString(",")
  }
}
