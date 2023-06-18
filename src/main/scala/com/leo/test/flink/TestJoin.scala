package com.leo.test.flink

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 09:55
  */
object TestJoin {
  def main(args: Array[String]): Unit = {
    val env1 = ExecutionEnvironment.getExecutionEnvironment
    val data1 = ListBuffer[(Int,String)]()
    data1.append((1,"asdf"))
    data1.append((2,"leo"))
    data1.append((3,"www"))

    val data2 = ListBuffer[(Int,String)]()
    data2.append((1,"shenzhen"))
    data2.append((2,"nanning"))
    data2.append((3,"guangzhou"))

    val text1 = env1.fromCollection(data1)
    val text2 = env1.fromCollection(data2)

    text1.join(text2).where(0).equalTo(0)
      .apply((first,second)=>{
      (first._1,first._2,second._2)
    }).print()

//    val env=ExecutionEnvironment.getExecutionEnvironment
//    val students=new ListBuffer[(Int,String)]
//    val students2=new ListBuffer[(Int,String)]
//    import org.apache.flink.api.scala._
//    students.append((1,"Hadoop"))
//    students2.append((1,"Spark"))
//    students.append((1,"Flink"))
//    students.append((2,"Java"))
//    students2.append((2,"Spring Boot"))
//    students.append((3,"Liunx"))
//    students2.append((4,"VUE"))
//    students.append((4,"VUE"))
//
//    val a1=env.fromCollection(students)
//    val a2=env.fromCollection(students2)
//    a2.join(a1).where(0).equalTo(0).apply{
//      (first,second) =>
//        (first._1,first._2,second._2)
//    }.print()

  }
}
