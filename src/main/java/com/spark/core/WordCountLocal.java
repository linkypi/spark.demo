package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;

public class WordCountLocal {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal.scala")
                .setMaster("local"); //spark://192.168.199.160:7077

        JavaSparkContext context = new JavaSparkContext(conf);
//        JavaRDD<String> lines = context.textFile("hdfs://172.23.0.2:9000/input/words.txt");
        JavaRDD<String> lines = context.textFile("/Users/leo/Documents/spark.projects/docker-compose/input_files/words.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1l;

            public Iterator<String> call(String line) throws Exception{
                return Arrays.asList(line.split(" ")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairRDD<String,Integer> topRdd = wordCounts.sortByKey(new Comparator<String>() {
            public int compare(String o1, String o2) {
                return 0;
            }
        });
        topRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared "+ wordCount._2 + " times");
            }
        });

        context.close();
    }
}
