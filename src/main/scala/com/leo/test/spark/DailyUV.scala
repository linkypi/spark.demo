package com.leo.test.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession, types}
import org.apache.spark.sql.functions._
/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-30 15:54
  */
object DailyUV {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("HiveDataSource")
      .enableHiveSupport()
      .getOrCreate()
    val sqlContext = spark.sqlContext
    val sparkContext = spark.sparkContext
    import spark.implicits._
//    import sqlContext.implicits._

    val accessLog = Array(
      "2019-08-28,1234",
      "2019-08-28,1234",
      "2019-08-29,1254",
      "2019-08-30,2238",
      "2019-08-30,4537",
      "2019-08-29,9832",
      "2019-08-27,1834",
      "2019-08-31,1934",
      "2019-08-26,1562",
    )

    val rdd = sparkContext.parallelize(accessLog,1)
    val accesslogrdd = rdd.map(log=>Row(log.split(",")(0),log.split(",")(1).toInt))

    val structTypes = types.StructType(Array(
      StructField("date",StringType,true),
      StructField("user_id",IntegerType,true)
    ))

    val rowDf = sqlContext.createDataFrame(accesslogrdd,structTypes)

    //import org.apache.spark.sql.functions.countDistinct
    rowDf.groupBy("date")
      .agg('date, countDistinct('user_id))
      .rdd.map(row=>Row(row(1),row(2)))
      .collect().foreach(println)

  }
}
