package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", 9999, "\n");

        SingleOutputStreamOperator<Tuple2<String, Integer>> words = env.socketTextStream("localhost", 9999, "\n", 3)
                .flatMap(new MyFlatMapper());
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = words.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            // 选取元组的第几个元素作为 key
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
        });

        // 对元组的第二个数据进行汇总
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedStream.sum(1);
        resultStream.print();
        env.execute("wordCount");
    }

    // 自定义类，实现FlatMapFunction接口
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {

        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            // 按照空格进行分词
            String[] words = value.split("\\s+");
            // 遍历所有word包成二元组
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
