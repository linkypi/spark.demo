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
import java.util.Iterator;

/**
 * # standalone client 模式
 * /opt/spark-3.3.2-bin-hadoop3/bin/spark-submit \
 * --class com.spark.core.WordCountCluster \
 * --master spark://node1:7077 \
 * --num-executors 1 \ # 共有多少个executer运行spark app
 * --driver-memory 800m \
 * --executor-memory 600m \
 * --executor-cores 1 \
 * ~/spark.test-1.0-SNAPSHOT.jar
 *
 * # standalone cluster 模式
 * /opt/spark-3.3.2-bin-hadoop3/bin/spark-submit \
 * --class com.spark.core.WordCountCluster \
 * --master spark://node1:7077 \
 * --deploy-mode cluster \
 * --supervise \ # 指定spark监控driver节点，若driver挂掉则自动重启driver
 * --num-executors 1 \
 * --driver-memory 800m \
 * --executor-memory 600m \
 * --executor-cores 1 \
 * ~/spark.test-1.0-SNAPSHOT.jar
 *
 * # yarn cluster mode
 * /opt/spark-3.3.2-bin-hadoop3/bin/spark-submit \
 * --class com.spark.core.WordCountCluster \
 * --master yarn \
 * --deploy-mode cluster \
 * --num-executors 1 \
 * --driver-memory 800m \
 * --executor-memory 600m \
 * --executor-cores 1 \
 * ~/spark.test-1.0-SNAPSHOT.jar
 */
public class WordCountCluster {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("WordCountCluster");
        // 若是以yarn cluster 模式执行则不可设置Master， 否则执行出错
                // .setMaster("local[2]")
                // .setMaster("spark://node1:7077");

        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile("hdfs://node1:9000/student_score.txt");

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1l;

            public Iterator<String> call(String line) throws Exception{
                return Arrays.asList(line.split(",")).iterator();
            }
        });

        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });

        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1 + " appeared "+ wordCount._2 + " times");
            }
        });

        context.close();
    }
}
