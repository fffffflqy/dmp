package com.test

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object parquet_Test {
    def main(args: Array[String]): Unit = {

        System.setProperty("hadoop.home.dir","D:\\app\\hadoop-2.7.7")
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer")
            .set("spark.sql.parquet.compression.codec", "snappy")
        val spark: SparkSession = SparkSession.builder()
            .master("local")
            .appName("dmp")
            .getOrCreate()

        val df: DataFrame = spark.read.parquet("out")
//        println(df.schema)
        df.createTempView("log")

        //sql
        val df2: DataFrame = spark.sql(
            """
              |select
              |count(*) sc,
              |provincename,
              |cityname
              |from log
              |group by provincename, cityname
              |""".stripMargin)

        df2.show()
        //dsl
        import spark.implicits._
        df.select($"provincename", $"cityname")
            .groupBy($"provincename", $"cityname").count().show()


//        df2.show()
//        df2.write.partitionBy("provincename","cityname").json("out3")
//        df2.coalesce(2).write.json("out2")
//        val rdd: RDD[((String, String), Int)] = df.rdd.map(t => {
//            ((t.getAs[String]("provincename"), t.getAs[String]("cityname")), 1)
//        }).reduceByKey(_ + _)
//        rdd.foreach(println)
//        spark.stop()
    }
}
