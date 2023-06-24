package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class StreamWordCountWithLambda {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple2<String, Integer>> words =
                env.socketTextStream("localhost", 9999, "\n", 3)
                .flatMap((String value, Collector<Tuple2<String, Integer>> out)->{
                    String[] ws = value.split("\\s+");
                    for(String item: ws){
                        out.collect(Tuple2.of(item, 1));
                    }
                });
//                .returns(TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}))
//                .returns(new TypeHint<Tuple2<String, Integer>>(){});

        // 选取元组的第几个元素作为 key
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = words.keyBy(
                (tuple2)-> tuple2.f0
        );

        // 对元组的第二个数据进行汇总
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = keyedStream.sum(1);
        resultStream.print();
        env.execute("wordCount");
    }

}
