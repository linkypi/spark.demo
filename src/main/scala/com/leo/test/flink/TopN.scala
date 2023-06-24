package com.leo.test.flink

import org.apache.flink.api.java.io.CsvInputFormat
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableEnvironment._
//import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment

import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.io.Path


/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 17:44
  */
object TopN {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)

    val url = TopN.getClass().getClassLoader.getResource("UserBehavior.csv")
    val file=Source.fromFile("E:\\scalaIO.txt")
    var list = new ListBuffer[UserBehavior]
    for(line <- file.getLines)
    {
        val arr = line.split(",")
        val model = new UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt, arr(3),arr(4).toLong)
        list += model
    }
    file.close

    val env = ExecutionEnvironment.getExecutionEnvironment
    val dataset:DataSet[(String,Integer)] = env.readCsvFile("")

//    val tableEnv = TableEnvironment.getTableEnvironment(env)
//    tableEnv.registerDataSet("tmp_table", dataset, 'user_id, 'item_id, 'category_id, 'behavior, 'timestamp)
//    val table = tableEnv.sqlQuery("select * from tmp_table where behavior='pv'")


//    val dataSource = env.createInput[UserBehavior](list)
  }
  case class UserBehavior(var userId:Long,var itemId:Long,
                          var categoryId:Int,var behavior: String,var timestamp: Long)
}
