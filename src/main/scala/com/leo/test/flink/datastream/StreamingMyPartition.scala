package com.leo.test.flink.datastream

import com.leo.test.flink.partition.MyPartition
import com.leo.test.flink.source.CustomSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 09:42
  */
object StreamingMyPartition {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new CustomSource)

    val tupleData = text.map(line=>{
      Tuple1(line)
    })

    val pdata = tupleData.partitionCustom(new MyPartition,0)

     val result = pdata.map(line=>{
       println("當前綫程Id: "+ Thread.currentThread().getId+" , value: "+ line)
     })

    result.print().setParallelism(1)
    env.execute("StreamingMyPartition")
  }
}
