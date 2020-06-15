package com.day1

class Afdsaf{

}

object tt {
    def main(args: Array[String]): Unit = {
        val a: Afdsaf = new Afdsaf()
        a.equals()

        implicit def fuck(a: Double):Int={
            val c = a-3
            c.toInt
        }
//        val a: Int = 4.6
//        println(a)
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

    def main(args: Array[String]): Unit = {

        /**
         * (1)bigger[T]为泛型函数
         * (2)bigger(...)(...)该函数是柯里化的
         * (3)第二个括号传入的是一个匿名函数，类型为T => Ordered[T] orders是隐式参数 输入类型为T类型， 返回类型为Ordered[T]类型
         *
         * */
        def bigger[T](a: T, b: T)(implicit ordered: T => Ordered[T]) = {
            /**
             * ordered(a) > b中的">"是一个函数 具体定义在Ordered类中
             * Source define:
             *        def >  (that: A): Boolean = (this compare that) >  0
             */
            if (a > b) a else b   // if (a > b) a else b  这样写也可以
        }

        println(bigger(4, 3))                 //4
//        println(bigger("Spark", "Hadoop"))    //Spark

    }
}