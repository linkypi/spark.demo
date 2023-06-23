package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.*;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 17:09
 */
public class MapPartitions {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("MapPartitions");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        List<String> studentNames = Arrays.asList("jack", "leo", "ben", "lily");
        JavaRDD<String> javaRDD = sparkContext.parallelize(studentNames, 2);

        final Map<String, Double> studentscoreMap = new HashMap<String, Double>();
        studentscoreMap.put("jack", 278.5);
        studentscoreMap.put("leo", 290.0);
        studentscoreMap.put("ben", 301.0);
        studentscoreMap.put("lily",200.5);

        // map 与 mapPartitions 区别：
        // map 是每次对一个partition中的一条数据做处理
        // mapPartitions 是每次对一个partition的所有数据做处理
        // 若数据量不是很大，建议使用 mapPartitions 替换 map
        // 若数据量很大，如10亿， 则不建议使用 mapPartitions ，因为可能会导致内存溢出
        JavaRDD<Double> doubleJavaRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {
            public Iterator<Double> call(Iterator<String> iterator) throws Exception {
                List<Double> scoreList = new ArrayList<Double>();
                while (iterator.hasNext()){
                    String stuName = iterator.next();
                    Double score = studentscoreMap.get(stuName);
                    scoreList.add(score);
                }
                return scoreList.iterator();
            }
        });

        for(Double item: doubleJavaRDD.collect()){
            System.out.println("===> "+ item);
        }

        sparkContext.close();
    }
}
