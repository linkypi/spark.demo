package com.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        // 批处理的数据抽象是一个 DataSet, 继承于 DataStream, 即批处理是基于流处理来实现
        DataSource<String> dataSet = batchEnv.readTextFile("hdfs://node1:9000/words.txt");
        AggregateOperator<Tuple2<String, Integer>> sum = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
                String[] words = s.split("\\s+");
                for (String item : words) {
                    collector.collect(Tuple2.of(item, 1));
                }
            }
        }).groupBy(0).sum(1);

        sum.print();
//        sum.count();
    }
}
