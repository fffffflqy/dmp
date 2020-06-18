package com.day1

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
 * 打广告标签
 */
object TagsAD extends Tags{
   def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
     val row: Row = args(0).asInstanceOf[Row]
     val adType = row.getAs[Int]("adspacetype")
     val adName = row.getAs[String]("adspacetypename")
     // 广告位类型（标签格式： LC0x->1 或者 LCxx->1）xx 为数字，
     // 小于 10 补 0，把广告位类型名称，LN 插屏->1
     adType match {
       case v if v>9 => list:+=("LC"+v,1)
       case v if v>0 && v<=9 => list:+=("LC0"+v,1)
     }
     // 保证名字不为空
     if(StringUtils.isNotBlank(adName)){
       list:+=("LN"+adName,1)
     }
     list
  }
}
