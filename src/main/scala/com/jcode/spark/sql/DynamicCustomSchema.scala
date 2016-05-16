package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL 各个数据类型
import org.apache.spark.sql.types.{ StructType, StructField, StringType };

/**
 * 编程方式定义Schema
 */
object DynamicCustomSchema {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("DynamicCustomSchema") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    // sc 是已有的SparkContext对象
    val sqlContext = new SQLContext(sc)

    // 创建一个RDD
    val people = sc.textFile("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/people.txt")

    // 将RDD[people]的各个记录转换为Rows，即：得到一个包含Row对象的RDD
    val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

    // 数据的schema被编码在一个字符串中
    val schemaString = "name age"

    // 基于前面的schemaString生成schema
    val schema =
      StructType(
        schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

    // 将schema应用到包含Row对象的RDD上，得到一个DataFrame
    val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    // 将DataFrame注册为table
    peopleDataFrame.registerTempTable("people")

    // 执行SQL语句
    val results = sqlContext.sql("SELECT name FROM people")

    // SQL查询的结果是DataFrame，且能够支持所有常见的RDD算子
    // 并且其字段可以以索引访问，也可以用字段名访问
    results.map(t => "Name: " + t(0)).collect().foreach(println)

  }

}