package com.day1

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 地域维度指标分析
 */
object LocationRpt {
    def main(args: Array[String]): Unit = {

        System.setProperty("hadoop.home.dir","D:\\app\\hadoop-2.7.7")
        // 参数判断

        val Array(inputPath)=args
        // 创建执行入口
        val conf = new SparkConf()
            .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
        val spark = SparkSession.builder()
            .appName("log2Parquet")
            .master("local")
            .config(conf) // 加载配置
            .getOrCreate()
            // 获取数据
         val df: DataFrame = spark.read.parquet(inputPath)
            // 注册临时视图
            df.createTempView("log")
            // 执行SQL语句
            val df2 = spark.sql(
              """
                |select
                |provincename,
                |cityname,
                |sum(case when requestmode =1 and processnode>=1 then 1 else 0 end) ysrequest,
                |sum(case when requestmode =1 and processnode>=2 then 1 else 0 end) yxrequest,
                |sum(case when requestmode =1 and processnode =3 then 1 else 0 end) adrequest,
                |sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) cybid,
                |sum(case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end) cybidsucc,
                |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end) shows,
                |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end) clicks,
                |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0 end) dspcost,
                |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0 end) dspapy
                |from log
                |group by provincename,cityname
                |""".stripMargin)

        //    // 将结果写入MySQL数据库
        //    // 加载配置文件（conf、json、properties）
            val load = ConfigFactory.load()
            val prop = new Properties()
            prop.setProperty("user",load.getString("jdbc.user"))
            prop.setProperty("password",load.getString("jdbc.password"))
            df2.coalesce(1).write.mode(SaveMode.Append).jdbc(
              load.getString("jdbc.url"),
              load.getString("jdbc.TabName"),
              prop
            )
        val list1 = List(List(1,2,3,4,5,6,7,8,9),List(1,2,3,4,5,6,7,8,9), List(1,2,3,4,5,6,7,8,9),List(1,2,3,4,5,6,7,8,9))
        val rdd = spark.sparkContext.parallelize(list1)
        val value: RDD[(Int, List[Int])] = rdd.map((1, _))
        value.reduceByKey((list1,list2)=>{
            // 提示用拉链
            val list = list1.zip(list2) // list((1,1),(2,2),(3,3),(4,4)....)
            list.map(t=>t._1+t._2) // 将数据聚合
        }).foreach(println)
        // 最终结果 List(2,4,6,8,10,12,14,16,18)
        spark.stop()
    }
}
