package com.day1

import java.lang.reflect.Field

import scala.reflect.runtime.universe

class Afdsaf{

}

object tt {
    def main(args: Array[String]): Unit = {


        implicit def fuck(str: String):Int={
//            Integer.parseInt(str)
            53
        }
        implicit def fuck2(str: String):Double={
            java.lang.Double.parseDouble(str)
//            5.6
        }

        val doubles: Array[String] = Array("", "23")
        fi(doubles(0), doubles(1))

    }
    def fi(str: Double, str2: Int): Unit ={
        println(str)
        println(str2)
    }

}


object Context_Implicits {
    implicit val default: String = "Java"
}

object Param {
    //函数中用implicit关键字 定义隐式参数
    def print(context: String)(implicit language: String){
        println(language+":"+context)
    }
}

object Implicit_Parameters {
    def main(args: Array[String]): Unit = {
        //隐式参数正常是可以传值的，和普通函数传值一样  但是也可以不传值，因为有缺省值(默认配置)
        Param.print("Spark")("Scala")   //Scala:Spark

        import Context_Implicits._
        //隐式参数没有传值，编译器会在全局范围内搜索 有没有implicit String类型的隐式值 并传入
        Param.print("Hadoop")          //Java:Hadoop
    }
}

class MyOrdered[T] extends Ordered[T]{
    override def compare(that: T): Int = 1

    override def >  (that: T): Boolean = (this compare that) <  0
}

object Implicit_Conversions_with_Implicit_Parameters {

//    def func()

    def main(args: Array[String]): Unit = {
        val s = Array(1, 2, 3)
//        val map= Map("fdaf" -> 2, "fdaffaa"->"fdf", "3rdef"->_)
//        map.
        for (i <- 1 to 3){
            println(i)
        }

//        val bb: BB = new BB(for(i <- 1 to 3)yield i)
    }
}

class BB(    val sessionid: Int,
             val advertisersid: Int,
             val adorderid: Int)
{
    def getSessionid()=sessionid
}