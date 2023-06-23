package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 17:32
 */
public class Sample {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Sample");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> studentNames = Arrays.asList("jack", "leo", "ben", "lily","ernest","张三","李四");
        JavaRDD<String> javaRDD = sparkContext.parallelize(studentNames);

        //从RDD 中随机抽取一定比例的数据
        JavaRDD<String> sampleRDD = javaRDD.sample(false, 0.2);

        for(String item: sampleRDD.collect()){
            System.out.println("===> "+ item);
        }

        sparkContext.close();
    }
}
