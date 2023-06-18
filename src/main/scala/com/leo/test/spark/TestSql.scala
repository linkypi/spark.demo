package com.leo.test.spark

import org.apache.spark.sql.execution.ui.SparkListenerDriverAccumUpdates
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession, types}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Desc
  * @Author linkypi
  * @Email trouble.linky@gmail.com
  * @Date 2019-08-29 16:27
  */
object TestSql {
  def main(args: Array[String]): Unit = {

//    RDDConvert2DataFrame
//    dynamicCreateMetadata
//    genericLoadSave
    getGt80Students
  }


  def genericLoadSave(): Unit ={
    val spark = SparkSession.builder()
//      .master("local")
      .appName("TestSql")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext

    val df = sqlContext.read.format("json").load("hdfs://spark1:9000/students.json") //E:\java\spark.test/students.json
    df.show()
    df.select("name","age","birthday","sex","salary")
      .write.format("parquet").save("hdfs://spark1:9000/students.parquet")
  }

  def getGt80Students(): Unit ={
    val spark = SparkSession.builder()
            .master("local")
      .appName("getGt80Students")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._

    val sdf = sqlContext.read.json("E:\\java\\spark.test/students.json")
    sdf.createOrReplaceTempView("student")
    val salaryDf = sqlContext.sql("select name, age from student where salary > 8000")
    val goodnames = salaryDf.rdd.map(row=>row(0)).collect()

    val studentInfoJson = Array("{\"name\": \"zhangsan\", \"level\": 1}",
      "{\"name\": \"lisi\", \"level\": 3}",
      "{\"name\": \"wangwu\", \"level\": 2}",
      "{\"name\": \"zhaoliu\", \"level\": 5}",
      "{\"name\": \"Tom\", \"level\": 7}"
    )

    val sinfordd = sparkContext.parallelize(studentInfoJson,1)
    val sinfoDf = sqlContext.read.json(sinfordd)
    sinfoDf.createOrReplaceTempView("student_info")

    var sql= "select name,level from student_info where name in ( "

    for(index <- 0 until goodnames.length){
      sql += "'" + goodnames(index) + "'"
      if(index<goodnames.length-1){
        sql += ","
      }
    }
    sql += ")"

    val goodStudentDf = sqlContext.sql(sql)

    val infoRow= goodStudentDf.rdd.map{row=>(row.getAs[String]("name"), row.getAs[Long]("level")) }
    val result = salaryDf.rdd.map(row=>(row.getAs[String]("name"), row.getAs[Long]("age"))).join(infoRow)

    val structType = StructType(Array(
      StructField("name",StringType,true),
      StructField("level",IntegerType,true),
      StructField("age",IntegerType,true),
    ))
    val rowRDD = result.map(info=>Row(info._2._1.toInt,info._2._2.toInt))
    val resultDF = sqlContext.createDataFrame(rowRDD,structType)
    resultDF.write.format("json").save("hdfs://spark1:9000/result.json")
//    resultDF.show()
  }

  //以编程方式动态指定元数据，将RDD转为DataFrame
  def dynamicCreateMetadata(): Unit ={
    val spark = SparkSession.builder()
      .master("local")
      .appName("TestSql")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext

    val studentRdd = sparkContext.textFile("E:\\java\\spark.test/students.txt",1)
      .map(line=>Row(line.split(",")(0),line.split(",")(1).toInt,line.split(",")(2),
                     line.split(",")(3),line.split(",")(4).toDouble) )
    val structType = StructType(Array(
      StructField("name",StringType,true),
      StructField("age",IntegerType,true),
      StructField("birthday",StringType,true),
      StructField("sex",StringType,true),
      StructField("salary",DoubleType,true)
    ))

    val sdf = sqlContext.createDataFrame(studentRdd,structType)
    sdf.createOrReplaceTempView("student")
    val temp = sqlContext.sql("select * from student where age>25")
    temp.rdd
      //      .map(row=>Student(row.getAs[String]("name"),row.getAs[Int]("age"),row.getAs[String]("birthday"),row.getAs[String]("sex"),row.getAs[Double]("salary")))
//      .map(arr=>Student(arr(0).toString,arr(1).toString.trim.toInt,arr(2).toString,arr(3).toString,arr(4).toString.trim.toDouble))
      .collect().foreach(item=>println(item.toString))
  }

  def RDDConvert2DataFrame(): Unit ={
    val spark = SparkSession.builder()
      .master("local")
      .appName("TestSql")
      .getOrCreate()
    val sparkContext = spark.sparkContext
    val sqlContext = spark.sqlContext
    import spark.implicits._

    val studentRdd = sparkContext.textFile("E:\\java\\spark.test/students.json",1)
      .map(line=>line.split(","))
      .map(arr=>Student(arr(0),arr(1).trim.toInt,arr(2),arr(3),arr(4).trim.toDouble))

    // RDD 转为 DataFrame
    val df = studentRdd.toDF()
    df.show()
    df.createOrReplaceTempView("student")
    val temp = sqlContext.sql("select * from student where age>25")

    temp.rdd
//      .map(row=>Student(row.getAs[String]("name"),row.getAs[Int]("age"),row.getAs[String]("birthday"),row.getAs[String]("sex"),row.getAs[Double]("salary")))
      .map(arr=>Student(arr(0).toString,arr(1).toString.trim.toInt,arr(2).toString,arr(3).toString,arr(4).toString.trim.toDouble))
      .collect().foreach(item=>println(item.toString))
  }

  case class Student(name:String, age: Int, birthday: String,sex: String,salary: Double)

  def testSql(): Unit ={

    val session = SparkSession.builder()
//      .master("local")
      .appName("TestSql")
      .getOrCreate()
    val sqlContext = session.sqlContext

    val df = sqlContext.read.json("hdfs://spark1:9000/students.json")
    df.show()

    df.printSchema()
    df.select("name").show()
    df.select(df.col("name"),df.col("age")+2).show()

    df.filter(df.col("age") > 25).show()
    df.groupBy("age").count().show()
  }
}
