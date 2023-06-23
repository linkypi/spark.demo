package com.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PlusClosureVar {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local")
//                .setMaster("spark://node1:7077")
                .setAppName("PlusClosureVar");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);

        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> numberRdd = sparkContext.parallelize(numbers);
        final List<Integer> closureResult = new ArrayList<Integer>();
        closureResult.add(0);
        numberRdd.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                Integer value = closureResult.get(0);
                value += integer;
                closureResult.set(0, value);
                System.out.println("闭包值汇总结果："+ closureResult.get(0));
            }
        });
        System.out.println("闭包值汇总结果："+ closureResult.get(0));
        sparkContext.close();
    }
}
