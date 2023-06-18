package com.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * 每隔5秒统计最近10秒内，每种类型每个商品的点击次数，然后统计出每个种类前三热门的商品
 */
public class Top3HotProduct {
    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("top3HotProduct");
        JavaStreamingContext jsContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        // 数据格式: username  product category
        // leo  iphone mobile
        // hack  iphone mobile
        // ben  iphone mobile
        // lily  redmi mobile
        // jim  nexus mobile
        // green  redmi mobile
        JavaReceiverInputDStream<String> searchLogDStream = jsContext.socketTextStream("localhost", 9999);
        JavaPairDStream<String, Integer> pairDStream = searchLogDStream.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(" ")[2] + "_" + s.split(" ")[1], 1);
            }
        });

        // 每隔 5 秒统计出最近 10 秒的数据, 计算出每个种类 每种商品的点击次数
        JavaPairDStream<String, Integer> windowDStream = pairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, Durations.seconds(10), Durations.seconds(5));


        windowDStream.foreachRDD(new VoidFunction2<JavaPairRDD<String, Integer>, Time>() {
            public void call(JavaPairRDD<String, Integer> rdd, Time time) throws Exception {
                JavaRDD<Row> categoryCountRDD = rdd.map(new Function<Tuple2<String, Integer>, Row>() {
                    public Row call(Tuple2<String, Integer> tuple2) throws Exception {
                        String category = tuple2._1.split(" ")[0];
                        String product = tuple2._1.split(" ")[1];
                        Integer count = tuple2._2;
                        return RowFactory.create(category, product, count);
                    }
                });

                // 执行 dataframe 转换
                List<StructField> fieldList = new ArrayList<StructField>();
                fieldList.add(DataTypes.createStructField("category", DataTypes.StringType, true));
                fieldList.add(DataTypes.createStructField("product", DataTypes.StringType, true));
                fieldList.add(DataTypes.createStructField("click_count", DataTypes.IntegerType, true));
                StructType structType = DataTypes.createStructType(fieldList);

                SQLContext sqlContext = new SQLContext(categoryCountRDD.context());
                Dataset<Row> dataFrame = sqlContext.createDataFrame(categoryCountRDD, structType);
                dataFrame.createTempView("product_click_log");
                Dataset<Row> sql = sqlContext.sql("" +
                        "select category, product, click_count from (" +
                        "  select category, product, click_count," +
                        "  row_number() over (partition by category order by click_count desc) rank" +
                        "  from product_click_log" +
                        ") temp where rank <=3");
                sql.show();
            }
        });

        jsContext.start();
        jsContext.awaitTermination();
        jsContext.close();
    }
}
