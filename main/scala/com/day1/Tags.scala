package com.day1

/**
 * 定义打标签的接口
 */
trait Tags {
  def makeTags(args:Any*):List[(String,Int)]
}
