package com.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Desc
 * @Author linkypi
 * @Email trouble.linky@gmail.com
 * @Date 2019-08-21 17:36
 */
public class TestCustomSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> text = environment.addSource(new CustomRichSource()).setParallelism(2);
//        DataStreamSource<Long> text = environment.addSource(new CustomParallelSource()).setParallelism(2);
        DataStream<Long> num = text.map(new MapFunction<Long, Long>(){

            public Long map(Long value)throws Exception{
                System.out.println("原始数据： " + value);
                return value;
            }
        }).filter(new FilterFunction<Long>() {
            public boolean filter(Long value) throws Exception {
                return value%2==0;
            }
        }).map(new MapFunction<Long, Long>(){

            public Long map(Long value)throws Exception{
                System.out.println("过滤后的数据： " + value);
                return value;
            }
        });

        DataStream<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        sum.print().setParallelism(2);
        environment.execute("test customparallelsource");
    }
}
