package com.tab

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagKeyWord extends Tags {
    override def makeTags(args: Any*): List[(String, Int)] = {
        var list = List[(String,Int)]()
        val row = args(0).asInstanceOf[Row]
        val stop = args(1).asInstanceOf[Broadcast[Array[String]]]
        // 获取关键字，进行过滤
        val kw = row.getAs[String]("keywords").split("\\|")
        kw.filter(word=>word.length>=3 && word.length<=8 && !stop.value.contains(word))
            .foreach(word=>{
                list:+=("K"+word,1)
            })
        list
    }
}
