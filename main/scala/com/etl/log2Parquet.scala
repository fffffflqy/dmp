package com.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object log2Parquet {
    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir","D:\\app\\hadoop-2.7.7")
        val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer")
            .set("spark.sql.parquet.compression.codec", "snappy")
        val spark: SparkSession = SparkSession.builder()
            .master("local")
            .appName("dmp")
            .getOrCreate()

        implicit def fuck(str: String):Int={
            if (str=="")0
            else java.lang.Integer.parseInt(str)
        }
        implicit def fuck1(str: String): Double={
            if (str=="")0
            else java.lang.Double.parseDouble(str)
        }
        val rdd: RDD[String] = spark.sparkContext.textFile("data\\textLog.log")
        val word: RDD[log] = rdd.filter(t => t.split(",", t.length).length >= 85).map(x => {
            val arr: Array[String] = x.split(",", -1)
//            println(arr.length)
            new log(
                arr(0),
                arr(1),
                arr(2),
                arr(3),
                arr(4),
                arr(5),
                arr(6),
                arr(7),
                arr(8),
                arr(9),
                arr(10),
                arr(11),
                arr(12),
                arr(13),
                arr(14),
                arr(15),
                arr(16),
                arr(17),
                arr(18),
                arr(19),
                arr(20),
                arr(21),
                arr(22),
                arr(23),
                arr(24),
                arr(25),
                arr(26),
                arr(27),
                arr(28),
                arr(29),
                arr(30),
                arr(31),
                arr(32),
                arr(33),
                arr(34),
                arr(35),
                arr(36),
                arr(37),
                arr(38),
                arr(39),
                arr(40),
                arr(41),
                arr(42),
                arr(43),
                arr(44),
                arr(45),
                arr(46),
                arr(47),
                arr(48),
                arr(49),
                arr(50),
                arr(51),
                arr(52),
                arr(53),
                arr(54),
                arr(55),
                arr(56),
                arr(57),
                arr(58),
                arr(59),
                arr(60),
                arr(61),
                arr(62),
                arr(63),
                arr(64),
                arr(65),
                arr(66),
                arr(67),
                arr(68),
                arr(69),
                arr(70),
                arr(71),
                arr(72),
                arr(73),
                arr(74),
                arr(75),
                arr(76),
                arr(77),
                arr(78),
                arr(79),
                arr(80),
                arr(81),
                arr(82),
                arr(83),
                arr(84)
            )
        })
        val arr= word
        import spark.implicits._
        // 如果我们使用的是类，而不是样例类，那么此时类最多只能使用22个参数字段
        // 那么想要用类使用超过22个字段，需要继承product特质
        val df: DataFrame = word.toDF()
        df.show()
//        word.foreach(println)
        // 将处理后的数据存入到存储系统（本地）
//        df.write.parquet("D:\\java\\dmp\\out")


    }

}
