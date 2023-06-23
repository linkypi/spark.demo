package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 18:35
 */
public class Repartition {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Repartition");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // repartition 算子用于增加或减少partition数量 而 coalesce 仅仅可以将partition减少
        // 经典场景即是 spark SQL从hive查询数据， spark SQL会根据hive
        // 对应的hdfs文件的block数量来决定RDD有多少个partition

        List<String> studentNames = Arrays.asList("jack", "leo", "ben", "lily", "ernest", "张三");
        JavaRDD<String> stuRDD = sparkContext.parallelize(studentNames, 2);

        JavaRDD<String> staffRDD = stuRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String staff = iterator.next();
                    String msg = MessageFormat.format("部门 {0}: {1}", (index + 1), staff);
                    list.add(msg);
                }
                return list.iterator();
            }
        }, true);

        for (String item : staffRDD.collect()) {
            System.out.println(item);
        }

        // 将6个部门缩减为3个部门
        JavaRDD<String> resultRDD = staffRDD.repartition(6);
        JavaRDD<String> staffRDD2 = resultRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()) {
                    String staff = iterator.next();
                    String msg = MessageFormat.format("部门 {0}: {1}", (index + 1), staff);
                    list.add(msg);
                }
                return list.iterator();
            }
        }, true);
        staffRDD2.count();
        for (String item : staffRDD2.collect()) {
            System.out.println(item);
        }
        sparkContext.close();
    }
}
