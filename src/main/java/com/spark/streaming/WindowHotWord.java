package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;




import java.util.List;

/**
 * 实时统计出现次数最多的前三个关键词
 * leo java
 * jim es
 * jack java
 * lily spark
 * lilei spark
 * enest spark
 * ben spark
 */
public class WindowHotWord {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("windowHotWord");
        JavaStreamingContext jsContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 数据格式: username  log
        JavaReceiverInputDStream<String> searchLogDStream = jsContext.socketTextStream("localhost", 9999);
        JavaDStream<String> mapDStream = searchLogDStream.map(new Function<String, String>() {
            public String call(String s) throws Exception {
                return s.split(" ")[1];
            }
        });
        // 将搜索词映射为 (word, 1) 的格式
        JavaPairDStream<String, Integer> pairDStream = mapDStream.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        // 对(word, 1)格式的DStream 执行 reduceByKeyAndWindow 滑动窗口操作
        JavaPairDStream<String, Integer> windowPairDStream = pairDStream.reduceByKeyAndWindow(
                new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
            // 窗口长度为 10 秒, 滑动间隔为两秒
        }, Durations.seconds(10), Durations.seconds(5));

        // 一个窗口就是一个 10 秒钟的数据变成一个 RDD
        // 然后对这个 RDD 根据每个搜索词出现的频率进行排序, 然后获取排名前三的搜索词
        JavaPairDStream<String, Integer> fianlDS = windowPairDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> javaPairRDD) throws Exception {
                // 将结构 (word, 1) 反转为 (1, word) 以便对词频进行排序
                JavaPairRDD<Integer, String> pairRDD = javaPairRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple2) throws Exception {
                        return new Tuple2<Integer, String>(tuple2._2, tuple2._1);
                    }
                });
                JavaPairRDD<Integer, String> sortedRDD = pairRDD.sortByKey(false);

                // 再次反转为 (word, 1)
                JavaPairRDD<String, Integer> finalRdd = sortedRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                        return new Tuple2<String, Integer>(tuple2._2, tuple2._1);
                    }
                });
                List<Tuple2<String, Integer>> top3 = finalRdd.take(3);
                for (Tuple2<String, Integer> item : top3) {
                    System.out.println(item._1() + " : " + item._2());
                }
                return finalRdd;
            }
        });

        fianlDS.print();

        jsContext.start();
        jsContext.awaitTermination();
        jsContext.close();
    }
}
