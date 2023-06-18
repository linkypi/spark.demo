package com.leo.test.spark

import org.apache.spark.sql.SparkSession

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-30 16:44
  */
object RowNumberFun {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
//      .master("local")
      .appName("HiveDataSource")
      .enableHiveSupport()
      .getOrCreate()
    import spark.sql

    // 查詢每种商品销售额排名前三的商品
    sql("DROP TABLE IF EXISTS sales")
    sql("CREATE TABLE IF NOT EXISTS sales(product string, category string, revenue bigint)")
    sql("LOAD DATA LOCAL INPATH '/root/spark.test/scala/hive/sales.txt' INTO TABLE sales")
    sql("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

    val top3Df = sql("select product,category,revenue from (" +
        "select product,category,revenue," +
        " row_number() OVER(PARTITION BY category ORDER BY revenue DESC) rank" +
        " from sales" +
      ") tmp where rank <=3")
    sql("DROP TABLE IF EXISTS top3_sales")
    top3Df.createOrReplaceTempView("top3_sales")
    spark.close()

  }
}
