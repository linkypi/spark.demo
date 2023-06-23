package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author: lynch
 * @description:
 * @date: 2023/6/23 18:16
 */
public class Coalesce {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("Coalesce");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        // coalesce 是将RDD 的partition缩减
        // 建议使用场景： 配合filter算子使用
        // 使用filter过滤数据后，会出现很多partition数据不均匀的情况
        // 此时建议使用 coalesce算子压缩partition数量，从而让各个partition中的数据更紧凑
        List<String> studentNames = Arrays.asList("jack", "leo", "ben", "lily", "ernest", "张三");
        JavaRDD<String> stuRDD = sparkContext.parallelize(studentNames, 6);

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
        JavaRDD<String> resultRDD = staffRDD.coalesce(3);
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
