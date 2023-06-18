package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class TransformBlacklist {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("transFormBlacklist");
        JavaStreamingContext jsContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 模拟一份黑名单RDD
        List<Tuple2<String, Boolean>> blacklist = new ArrayList<Tuple2<String, Boolean>>();
        blacklist.add(new Tuple2<String, Boolean>("tom", true));
        blacklist.add(new Tuple2<String, Boolean>("jack", true));
        blacklist.add(new Tuple2<String, Boolean>("ben", true));
        final JavaPairRDD<String, Boolean> blacklistRDD = jsContext.sparkContext().parallelizePairs(blacklist);

        // 此处仅做简化处理 格式是 date  username
        JavaReceiverInputDStream<String> adsDStream = jsContext.socketTextStream("localhost", 9999);
        // 对数据数据转换为 (username, date username), 以便与黑名单的 username 进行 join 操作
        JavaPairDStream<String, String> adsPairDStream = adsDStream.mapToPair(new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String s) throws Exception {
                return new Tuple2<String, String>(s.split(" ")[1], s);
            }
        });
        // 执行 transform 操作, 将 adsPairRDD 与 blacklistRDD 进行 join 操作
        JavaDStream<String> transformRDD = adsPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            public JavaRDD<String> call(JavaPairRDD<String, String> userPairRDD) throws Exception {

                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinRDD = userPairRDD.leftOuterJoin(blacklistRDD);
                // 连接之后进行 filter 过滤
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filterRDD = joinRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    // tuple 即为每个用户的访问日志和在黑名单的状态
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        // (username, date username) 左连接 (username , true/false) 得到的结果
                        if (tuple._2._2().isPresent() && tuple._2._2.get()) {
                            return false;
                        }
                        return true;
                    }
                });
                // 此时就剩下未被过滤的用户信息
                JavaRDD<String> validAds = filterRDD.map(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>() {
                    public String call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        return tuple._2._1();
                    }
                });

                return validAds;
            }
        });

        transformRDD.print();

        jsContext.start();
        jsContext.awaitTermination();
        jsContext.close();
    }
}
