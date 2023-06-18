package com.leo.test.flink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable.ListBuffer

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 11:26
  */
object TestBroadCast {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val broadData = ListBuffer[(String,Int)]()
    broadData.append(("ss",18))
    broadData.append(("dsda",28))
    broadData.append(("wuwang",21))

    val data = env.fromCollection(broadData)
    val castdata = data.map(value=>{
      Map(value._1 -> value._2)
    })

    val text = env.fromElements("ss","dsda","wuwang")
    val result = text.map(new RichMapFunction[String,String] {

      var list: java.util.List[Map[String,Int]] = null
      var allMap = Map[String,Int]()

      override def map(value: String): String = {
        val age = allMap.get(value).get
        value+ ", "+ age
      }

      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        list  = getRuntimeContext.getBroadcastVariable[Map[String,Int]]("broadcastmap")
        val it = list.iterator()
        while(it.hasNext){
          val next = it.next()
          allMap = allMap.++(next)
        }


      }
    }).withBroadcastSet(castdata,"broadcastmap")

    result.print()
  }
}
