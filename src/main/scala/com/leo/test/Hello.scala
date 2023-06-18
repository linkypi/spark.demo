package com.leo.test

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-19 16:30
  */
object Hello {
  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder().appName("TEST")
            .master("local[*]").getOrCreate()

    val fileRdd = spark.sparkContext.textFile("in",2)

    fileRdd.saveAsTextFile("output")


    // 提取器
//    class Person(val name:String, val age: Int)
//    object Person{
//      def apply( name: String , age: Int ): Person = new Person( name, age)
//
//      def unapply(str: String)= {
//         val index = str.indexOf(" ")
//         if(index == -1) None
//         else Some((str.substring(0,index),str.substring(index+1)))
//      }
//    }
//
//    val Person(name,age) = "leo 25"

    // 正则
//    val pattern = "[a-z]+".r
//    val str = "hello 1234 you "
//    for(item <- pattern.findAllIn(str)){
//      println(item)
//    }

    //    test
//    val sayFunc = (name:String)=> println("hello "+ name)
//    def greeting(func:(String)=>Unit, name:String){ func(name)}
//    greeting(sayFunc, "leo")

    // 内部类获取到外部类的引用
//    val c1 = new Class("c1")
//    val leo = c1.register("leo")
//    print(leo.myself)

    // 偏函数
//    val getStudentGrade : PartialFunction[String,Int] = {
//      case "leo" =>90
//      case "jack" => 88
//      case "john" => 79
//    }
//
//    println(getStudentGrade("john"))
//    println(getStudentGrade.isDefinedAt("tom"))
  }

  // 通过outer 可让内部类获取到外部类的引用
  class Class(val name:String){ outer =>

    class Student(val name:String){
      def myself = "I'm " + name + " in class " + outer.name
    }

    def register(name: String) ={
      new Student(name)
    }

  }



  def test = {
    println("Hello, Scala")

    val sum = (a: Int, b: Int) => {
      a + b
    }

    println("a+b: " + sum(12, 2))


    var arr = Array(1, 2, 3, 4, 5)
    arr = arr.filter(_ % 2 == 0).map(2 * _)
    val arr2 = for (item <- arr) yield item * item

    for (item <- arr2) println(item)

    val buf = ArrayBuffer[Int]()
    buf += (1, 2, 3, 4, 5)
  }
}
