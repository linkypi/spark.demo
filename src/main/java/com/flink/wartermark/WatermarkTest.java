package com.flink.wartermark;

import com.flink.ClickSource;
import com.flink.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * @author leo
 * @ClassName WatermarkTest
 * @description: TODO
 * @date 7/8/23 1:22 PM
 */
public class WatermarkTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.fromElements();
        SingleOutputStreamOperator<Event> streamOperator = env.addSource(new ClickSource())
                // 有序流的 waterMark 生成
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp)
                );
                // 乱序流的 watermark
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(200))
//                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (event, l) -> event.timestamp)
//                );
      streamOperator.map((Event event)->{
          return Tuple2.<String, Long>of(event.user, 1L);
      })
              .returns(new TypeHint<Tuple2<String, Long>>(){}) // 必须指定返回类型,否则 Java 无法识别返回值类型
              .keyBy(data->data.f0)
              .window(TumblingEventTimeWindows.of(Time.seconds(1)))
              .reduce((Tuple2<String, Long> t1,Tuple2<String, Long> t2)->{
                  return Tuple2.of(t1.f0, t1.f1 + t2.f1);}
                  ).print();


//        DataStreamSource<Event> stream = (DataStreamSource<Event>)
//        WindowedStream<Event, String, TimeWindow> window = stream.keyBy(data -> data.user)
////                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(5))) // 滑动事件时间窗口
////                .window(EventTimeSessionWindows.withGap(Time.minutes(1))) // 事件时间会话窗口
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)));// 滚动事件时间窗口
      env.execute();
    }
}
