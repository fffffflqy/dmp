package com.day1

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 上下文标签-> 用于合并总标签
 */
object TagsContext {
  def main(args: Array[String]): Unit = {

    val Array(inputPath)= args
    val conf = new SparkConf()
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .appName("log2Parquet")
      .master("local")
      .config(conf) // 加载配置
      .getOrCreate()
    // 获取数据
    val df: DataFrame = spark.read.parquet("out")
    // 判断用户的唯一ID必须要存在
    df.filter(TagUtils.OneUserId)
      // 进行打标签处理
      .rdd.map(row=>{
      // 获取不为空的唯一UserId
      val userId: String = TagUtils.getAnyOneUserId(row)
      // 广告类型标签
      val adList: List[(String, Int)] = TagsAD.makeTags(row)
      ()
    })
  }
}
