package com.leo.test.flink.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-22 09:18
  */
class CustomSource extends SourceFunction[Long]{

  var count = 1
  var running = true

  override def run(sourceContext: SourceFunction.SourceContext[Long]): Unit = {
     while (running){
       sourceContext.collect(count)
       count += 1
       Thread.sleep(300)
     }
  }

  override def cancel(): Unit = {
     running = false
  }
}
