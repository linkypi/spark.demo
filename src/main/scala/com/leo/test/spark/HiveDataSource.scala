package com.leo.test.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-30 10:57
  */
object HiveDataSource {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("HiveDataSource")
      .enableHiveSupport()
      .getOrCreate()
    val sqlContext = spark.sqlContext
    val sparkContext = spark.sparkContext

    import spark.implicits._
    import spark.sql

    var prefix = "E:\\java\\spark.test" //hdfs://spark1:9000

    sql("DROP TABLE IF EXISTS student_infos")
    sql("CREATE TABLE IF NOT EXISTS student_infos(name string, age int, birthday string, sex string, salary long)")
    sql("LOAD DATA LOCAL INPATH '"+ prefix +"/students.txt' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' " +
    "INTO TABLE student_infos")

    //student_score.txt
    sql("DROP TABLE IF EXISTS student_score")
    sql("CREATE TABLE IF NOT EXISTS student_score(name string, score int)")
    sql("LOAD DATA LOCAL INPATH '"+ prefix +"/student_score.txt' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'" +
      "INTO TABLE student_score")

    // join
    val df = sql("select a.name, a.age, a.birthday, a.sex, b.score " +
      " from student_infos a join student_score b on a.name=b.name where a.salary>8000")
    df.show()
    spark.close()
  }
}
