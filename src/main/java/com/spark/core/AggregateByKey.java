package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 17:52
 */
public class AggregateByKey {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("AggregateByKey");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> stringJavaRDD = sparkContext.textFile("D:\\spark.projects\\spark.test\\words.txt");
        JavaRDD<String> flatMapRDD = stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        JavaPairRDD pairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        // reduceByKey 是 aggregateByKey 的简化版
        // aggregateByKey 多提供一个函数 Seq Function, 即可以控制如何对每个partition中的数据进行聚合
        // 再对所有partition中的数据进行全局聚合
        JavaPairRDD<String, Integer> resultRDD = pairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        for (Tuple2<String, Integer> item: resultRDD.collect()) {
            System.out.printf(" %s: %s\n" , item._1, item._2);
        }

        sparkContext.close();
    }
}
