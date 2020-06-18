package com.test

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object LocationRpt02 {


    def main(args: Array[String]): Unit = {
        push4()

    }






    def push1(): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\app\\hadoop-2.7.7")
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer")
            .set("spark.sql.parquet.compression.codec", "snappy")
        val spark: SparkSession = SparkSession.builder()
            .master("local")
            .appName("dmp")
            .getOrCreate()
        val df: DataFrame = spark.read.parquet("out")
        df.createTempView("log")
        // 执行SQL语句
        val df2 = spark.sql(
            """
              |select
              |appname,
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
              |group by appname
              |""".stripMargin)
        df2.show()
        val config: Config = ConfigFactory.load()
        val prop: Properties = new Properties()
        prop.setProperty("user", config.getString("jdbc.user"))
        prop.setProperty("password", config.getString("jdbc.password"))
        df2.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(
            config.getString("jdbc.url"),
            "local2",
            prop
        )

    }

    def push2(): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\app\\hadoop-2.7.7")
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer")
            .set("spark.sql.parquet.compression.codec", "snappy")
        val spark: SparkSession = SparkSession.builder()
            .master("local")
            .appName("dmp")
            .getOrCreate()
        val df: DataFrame = spark.read.parquet("out")
        df.createTempView("log")
        // 执行SQL语句
        val df2 = spark.sql(
            """
              |select
              |networkmannername,
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
              |group by networkmannername
              |""".stripMargin)
        df2.show()
        val config: Config = ConfigFactory.load()
        val prop: Properties = new Properties()
        prop.setProperty("user", config.getString("jdbc.user"))
        prop.setProperty("password", config.getString("jdbc.password"))
        df2.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(
            config.getString("jdbc.url"),
            "local3",
            prop
        )

    }

    def push3(): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\app\\hadoop-2.7.7")
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer")
            .set("spark.sql.parquet.compression.codec", "snappy")
        val spark: SparkSession = SparkSession.builder()
            .master("local")
            .appName("dmp")
            .getOrCreate()
        val df: DataFrame = spark.read.parquet("out")
        df.createTempView("log")
        // 执行SQL语句
        val df2 = spark.sql(
            """
              |select
              |(case when client=1 then "android" when client=2 then "ios" when client=3 then "wp" else "wp" end) client,
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
              |group by client
              |""".stripMargin)
        df2.show()
        val config: Config = ConfigFactory.load()
        val prop: Properties = new Properties()
        prop.setProperty("user", config.getString("jdbc.user"))
        prop.setProperty("password", config.getString("jdbc.password"))
        df2.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(
            config.getString("jdbc.url"),
            "local4",
            prop
        )

    }

    def push4(): Unit = {
        System.setProperty("hadoop.home.dir", "D:\\app\\hadoop-2.7.7")
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer")
            .set("spark.sql.parquet.compression.codec", "snappy")
        val spark: SparkSession = SparkSession.builder()
            .master("local")
            .appName("dmp")
            .getOrCreate()
        val df: DataFrame = spark.read.parquet("out")
        df.createTempView("log")
        // 执行SQL语句
        val df2 = spark.sql(
            """
              |select
              |appname,
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
              |group by uuid
              |""".stripMargin)
        df2.show()
        val config: Config = ConfigFactory.load()
        val prop: Properties = new Properties()
        prop.setProperty("user", config.getString("jdbc.user"))
        prop.setProperty("password", config.getString("jdbc.password"))
        df2.coalesce(1).write.mode(SaveMode.Overwrite).jdbc(
            config.getString("jdbc.url"),
            "local5",
            prop
        )

    }

        //        val list1 = List(List(1,2,3,4,5,6,7,8,9),List(1,2,3,4,5,6,7,8,9))
        //        val rdd = spark.sparkContext.parallelize(list1)
        //        val value: RDD[(Int, List[Int])] = rdd.map((1, _))
        ////        value.foreach(println)
        //        value.reduceByKey((list1,list2)=>{
        //            for (i<-list1.zip(list2)) yield {i._2+i._1}
        //        }).foreach(println)

}
