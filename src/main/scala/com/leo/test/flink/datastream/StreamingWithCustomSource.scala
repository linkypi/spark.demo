package com.leo.test.flink.datastream

import com.leo.test.flink.source.CustomSource
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 09:22
  */
object StreamingWithCustomSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.addSource(new CustomSource)
    val data = text.map(line=>{
      println("數據：" + line)
      line
    }).filter(_%2==0)

    val sunm = data.map(line=>{
      println("清洗后的數據：" + line)
      line
    }).timeWindowAll(Time.seconds(2)).sum(0)
    sunm.print().setParallelism(1)
    env.execute("StreamingWithCustomSource")
  }
}
