package com.leo.test.flink.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 09:13
  */
object StreamingFromCollection {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = List(10,15,20)
    val text = env.fromCollection(data)

    val num = text.map(_+1)
    num.print().setParallelism(1)
    env.execute("streamingFromCollection")
  }
}
