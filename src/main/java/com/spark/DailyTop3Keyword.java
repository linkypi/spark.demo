package com.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveContextAwareRecordReader;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class DailyTop3Keyword {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("DailyTop3Keyword")
                .enableHiveSupport()
                .getOrCreate();

        SparkContext sparkContext = spark.sparkContext();
        SQLContext sqlContext = spark.sqlContext();
        HiveContext hiveContext = new HiveContext(sparkContext);
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sparkContext);

        Map<String, List<String>> queryParams = new HashMap<String, List<String>>();
        queryParams.put("city", Arrays.asList("beijing"));
        queryParams.put("platform", Arrays.asList("android"));
        queryParams.put("version", Arrays.asList("1.0","1.2","1.5","2.0"));

        final Broadcast<Map<String,List<String>>> broadcast = jsc.broadcast(queryParams);

        JavaRDD<String> rowRdd = jsc.textFile("hdfs://spark1:9000/keyword.txt",3);
        JavaRDD<String> filterRdd = rowRdd.filter(new Function<String, Boolean>() {
            public Boolean call(String line) throws Exception {
                // 日期 用户 搜索词 城市 平台 版本
                String[] arr = line.split("\t");

                String city = arr[3];
                String platform = arr[4];
                String version = arr[5];

                Map<String, List<String>> queryParams = broadcast.value();

                List<String> cities = queryParams.get("city");
                if (cities == null || cities.size() == 0 || !cities.contains(city)) {
                    return false;
                }
                List<String> platforms = queryParams.get("platform");
                if (platforms == null || platforms.size() == 0 || !platforms.contains(platform)) {
                    return false;
                }
                List<String> versions = queryParams.get("version");
                if (versions == null || versions.size() == 0 || !versions.contains(version)) {
                    return false;
                }

                return true;
            }
        });

        JavaPairRDD<String,String> dateKeyUserRdd = filterRdd.mapToPair(
           new PairFunction<String, String, String>() {
            public Tuple2<String, String> call(String log) throws Exception {
                // 日期 用户 搜索词 城市 平台 版本
                String[] arr = log.split("\t");

                String date = arr[0];
                String user = arr[1];
                String keyword = arr[2];


                return new Tuple2<String, String>(date + "_" + keyword, user);
            }
        });

        //进行分组
        JavaPairRDD<String,Iterable<String>> groupRdd = dateKeyUserRdd.groupByKey();
        JavaPairRDD<String,Long> UVRdd = groupRdd.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, String, Long>() {

            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> dateKeyUsers) throws Exception {
                String dateKeyword = dateKeyUsers._1;
                Iterator<String> users = dateKeyUsers._2.iterator();

                //对用户进行去重,并统计去重后的数量
                List<String> distinctUsers = new ArrayList<String>();

                while(users.hasNext()){
                    String user = users.next();
                    if(!distinctUsers.contains(user)){
                        distinctUsers.add(user);
                    }
                }

                // 获取UV
                long uv = distinctUsers.size();
                return new Tuple2<String, Long>(dateKeyword,uv);
            }
        });

        JavaRDD<Row> uvRowRdd = UVRdd.map(new Function<Tuple2<String, Long>, Row>() {
            public Row call(Tuple2<String, Long> dateKeyUv) throws Exception {
                String date = dateKeyUv._1.split("_")[0];
                String keyword = dateKeyUv._1.split("_")[1];
                long uv = dateKeyUv._2;


                return new RowFactory().create(date,keyword,uv);
            }
        });


        List<StructField> fields = Arrays.asList(
                DataTypes.createStructField("date",DataTypes.StringType,true),
                DataTypes.createStructField("keyword",DataTypes.StringType,true),
                DataTypes.createStructField("uv",DataTypes.LongType,true)
        );

        final StructType structType = DataTypes.createStructType(fields);

        Dataset<Row> dataFrame = sqlContext.createDataFrame(uvRowRdd,structType);
        dataFrame.createOrReplaceTempView("daily_keyword_uv");

        Dataset<Row> resultRdd = sqlContext.sql("select date,keyword,uv from (" +
                  " select date,keyword,uv,"+
                  " rownumber() over(partition by date order by uv desc ) rank" +
                  " from daily_keyword_uv" +
                ") tmp where rank<=3");

        JavaPairRDD<String,String> result = resultRdd.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                String date = String.valueOf(row.get(0)) ;
                String keyword = String.valueOf(row.get(1)) ;
                Long uv = Long.valueOf(String.valueOf(row.get(2))) ;

                return new Tuple2<String, String>(date,keyword+"_"+uv);
            }
        });

        JavaPairRDD<String,Iterable<String>> top3KeywordRdd = result.groupByKey();
        JavaPairRDD<Long,String> uvdateKeyRdd = top3KeywordRdd.mapToPair(new PairFunction<Tuple2<String, Iterable<String>>, Long, String>() {
            public Tuple2<Long, String> call(Tuple2<String, Iterable<String>> tuple) throws Exception {

                String date = tuple._1;
                Iterator<String> keywordUV = tuple._2.iterator();
                String datekeywords = date;

                Long totalUv = 0L;
                while (keywordUV.hasNext()){
                    String keyUV = keywordUV.next();
                    Long uv = Long.valueOf(keyUV.split("_")[1]);
                    datekeywords += "," + keyUV;
                    totalUv += uv;
                }

                return new Tuple2<Long, String>(totalUv,datekeywords);
            }
        });

        JavaPairRDD<Long,String> sortedRdd = uvdateKeyRdd.sortByKey(false);

        JavaRDD<Row> rowsRdd = sortedRdd.flatMap(new FlatMapFunction<Tuple2<Long, String>, Row>() {
            public Iterator<Row> call(Tuple2<Long, String> tuple) throws Exception {

                String kv = tuple._2;
                String[] arr = kv.split(",");
                String date = arr[0];

                List<Row> rows = new ArrayList<Row>();

                rows.add(RowFactory.create(date,arr[1].split("_")[0],Long.valueOf(arr[1].split("_")[1])));
                rows.add(RowFactory.create(date,arr[2].split("_")[0],Long.valueOf(arr[2].split("_")[1])));
                rows.add(RowFactory.create(date,arr[3].split("_")[0],Long.valueOf(arr[3].split("_")[1])));
                return rows.iterator();
            }
        });

        Dataset<Row> finalFrame = sqlContext.createDataFrame(rowsRdd,structType);
        finalFrame.write().saveAsTable("daily_top3_keyword_uv");

        spark.close();
    }

}
