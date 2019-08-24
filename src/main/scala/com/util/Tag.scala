package com.util

/**
  * Description: 打标签的统一接口
  *
  * @author Law
  * @version 1.0, 2019/8/23
  */
trait Tag {

  /**
    * 打标签的方法
    */
  def makeTags(args: Any*): List[(String, Int)]
}
