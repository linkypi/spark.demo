package com.leo.test.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-29 14:20
  */
object WordCountCluster {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("hdfs://spark1:9000/README.txt")
    val words = lines.flatMap(line=>line.split(" "))
    val pairs = words.map(word => (word,1))
    val wordCount = pairs.reduceByKey(_ + _)

    wordCount.foreach(item=>println(item._1 + " appears "+ item._2 + " times"))



  }
}
