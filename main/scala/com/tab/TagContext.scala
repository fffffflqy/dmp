package com.tab

import com.day1.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object TagContext {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        val spark = SparkSession.builder()
            .appName("log2Parquet")
            .master("local")
            .config(conf) // 加载配置
            .getOrCreate()
        // 获取数据
        val df: DataFrame = spark.read.parquet("out")
        val dicmap: RDD[(String, String)] = spark.sparkContext.textFile("data\\app_dict.txt")
            .map(x => x.split("\t", -1))
            .filter(_.length > 5)
            .map(x => (x(4), x(1)))
        val mapBroad = spark.sparkContext.broadcast(dicmap)
        // 读取停用词库
        val arr = spark.sparkContext.textFile("data\\stopwords.txt").collect()
        val stopBroad: Broadcast[Array[String]] = spark.sparkContext.broadcast(arr)
        // 获取数据
        df.filter(TagUtils.OneUserId).rdd
            .map(row=>{
                val userid: String = TagUtils.getAnyOneUserId(row)
                val adlist: List[(String, Int)] = TagsAD.makeTags(row)
                val appname: List[(String, Int)] = TagAPP.makeTags(row)
                val devlist: List[(String, Int)] = TagDev.makeTags(row)
                val kwlist: List[(String, Int)] = TagKeyWord.makeTags(row, stopBroad)
                devlist
            })
    }
}
