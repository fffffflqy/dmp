package com.tab

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsAD extends Tags {
    override def makeTags(args: Any*): List[(String, Int)] = {
        var li: List[(String, Int)] = List[(String, Int)]()
        val row: Row = args(0).asInstanceOf[Row]
        val adtype: Int = row.getAs[Int]("adspacetype")
        val adname: String = row.getAs[String]("adspacetypename")
        adtype match {
            case v if v > 9 => li:+=("LC"+v, 1)
            case v if v >= 0 && v<=9 => li:+=("LC0"+v, 1)
        }
        if (StringUtils.isNoneBlank(adname)){
            li:+=("LN"+adname, 1)
        }
        val channel = row.getAs[Int]("adplatformproviderid")
        li:+=("CN"+channel,1)
        // 地域标签
        val provincename = row.getAs[String]("provincename")
        val cityname = row.getAs[String]("cityname")
        if(StringUtils.isNoneBlank(provincename)){
            li:+=("ZP"+provincename,1)
            li:+=("ZC"+cityname,1)
        }
        li
    }
}
