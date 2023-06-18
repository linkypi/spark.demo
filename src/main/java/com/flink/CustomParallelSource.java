package com.flink;
import	java.util.Map;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.collection.Parallel;

/**
 * @Desc
 * @Author linkypi
 * @Email trouble.linky@gmail.com
 * @Date 2019-08-21 17:23
 */
public class CustomParallelSource implements ParallelSourceFunction<Long> {

    private long count = 1l;
    private boolean running = true;

    public void run(SourceContext<Long> sourceContext) throws Exception {
       while (running) {
           sourceContext.collect(count);
           count++;
           Thread.sleep(1000);
       }
    }

    public void cancel() {
       running = false;
    }
}
