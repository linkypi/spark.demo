package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.*;

/**
 * @Desc
 * @Author linkypi
 * @Email trouble.linky@gmail.com
 * @Date 2019-08-29 15:08
 */
public class TestJson {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TestJson");
        JavaSparkContext context = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(context);

//        StructType schemas = new StructType();
//        schemas.add(new StructField("name", DataTypes.StringType,true,null));
//        schemas.add(new StructField("age", DataTypes.IntegerType,true,null));
//        schemas.add(new StructField("birthday", DataTypes.StringType,true,null));
//        schemas.add(new StructField("salary", DataTypes.IntegerType,true,null));
//        schemas.add(new StructField("sex", DataTypes.IntegerType,true,null));

        Dataset<Row> df = sqlContext.read().json("hdfs://spark1:9000/students.json");
        df.show();

    }



}
