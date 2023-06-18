package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class UpdateStateByKey {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("updateStateByKey");
        JavaStreamingContext jsContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 如果要使用updateStateByKey算子，就必须设置一个checkpoint目录，
        // 开启checkpoint机制这样的话才能把每个key对应的state除了在内存中有，
        // 那么是不是也要checkpoint一份因为你要长期保存一份key的state的话，
        // 那么spark streaming是要求必须用checkpoint的，
        // 以便于在内存数据丢失的时候，可以从checkpoint中恢复数据
        jsContext.checkpoint("hdfs://spark1:9000/wc_checkpoint");

        JavaReceiverInputDStream<String> javaReceiverInputDStream = jsContext.socketTextStream("localhost", 9999);
        JavaDStream<String> lineDs = javaReceiverInputDStream.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairDStream<String, Integer> pairDStream = lineDs.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> resultDs = pairDStream.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
            // 这里两个参数实际上，对于每个单词，每次batch计算的时候，都会调用该函数第一个参数，
            // values，相当于是这个batch中，这个key的新的值，可能有多个,
            // 如一个hello，可能有2个1，(hello，1) (he1o，1)，那么传入的是(1,1)
            // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型可自定义
            public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
                // 定义一个全局计数
                Integer newVla = 0;

                if (state.isPresent()) {
                    newVla = state.get();
                }

                // 将key每次出现的次数累加
                for (Integer item : values) {
                    newVla += item;
                }
                return Optional.of(newVla);
            }
        });

        resultDs.print();

        jsContext.start();
        jsContext.awaitTermination();
        jsContext.close();
    }
}


