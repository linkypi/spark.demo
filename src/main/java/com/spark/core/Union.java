package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 17:32
 */
public class Union {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Union");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> studentNames = Arrays.asList("jack", "leo", "ben", "lily","ernest","张三","李四");
        JavaRDD<String> stuRDD = sparkContext.parallelize(studentNames);

        List<String> departments = Arrays.asList("develop", "market", "finance", "ops");
        JavaRDD<String> depRDD = sparkContext.parallelize(departments);

        JavaRDD<String> unionRdd = stuRDD.union(depRDD);
        for(String item: unionRdd.collect()){
            System.out.println("===> "+ item);
        }

        sparkContext.close();
    }
}
