package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 17:42
 */
public class GetUVByDistinct {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("GetUVByDistinct");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> accessLogs = Arrays.asList(
                "user1 2023-01-01 23:46:41",
                "user1 2023-01-01 23:38:32",
                "user1 2023-01-01 23:25:56",
                "user2 2023-01-01 23:42:41",
                "user2 2023-01-01 23:19:22",
                "user3 2023-01-01 23:58:42",
                "user4 2023-01-01 23:13:23",
                "user5 2023-01-01 23:58:31",
                "user6 2023-01-01 23:16:52",
                "user6 2023-01-01 23:29:15"
                );
        JavaRDD<String> logsRDD = sparkContext.parallelize(accessLogs);


        JavaRDD<String> userRDD = logsRDD.map(new Function<String, String>() {
            public String call(String v1) throws Exception {
                return v1.split(" ")[0];
            }
        });
        JavaRDD<String> distinct = userRDD.distinct();

        for(String item: distinct.collect()){
            System.out.println("===> "+ item);
        }

        sparkContext.close();
    }
}
