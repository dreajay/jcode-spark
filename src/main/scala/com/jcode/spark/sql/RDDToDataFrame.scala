package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 利用反射推导schema
 */
object RDDToDataFrame {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("RDDToDataFrame") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // 引入支持RDD到DataFrame的隐式转换
    import sqlContext.implicits._

    // 创建一个包含Person对象的RDD，toDF()将其转换为 DataFrame
    val people = sc.textFile("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/people.txt") //
      .map(_.split(",")) //
      .map(p => Person(p(0), p(1).trim.toInt)) //
      .toDF()
      
    //将其注册成table
    people.registerTempTable("people")

    // sqlContext.sql方法可以直接执行SQL语句
    val teenagers = sqlContext.sql("SELECT name, age FROM people WHERE age >= 13 AND age <= 19")

    // SQL查询的返回结果是一个DataFrame，且能够支持所有常见的RDD算子
    // 查询结果中每行的字段可以按字段索引访问:
    teenagers.map(t => "Name: " + t(0)).collect().foreach(println)

    // 或者按字段名访问:
    teenagers.map(t => "Name: " + t.getAs[String]("name")).collect().foreach(println)

    // row.getValuesMap[T] 会一次性返回多列，并以Map[String, T]为返回结果类型
    teenagers.map(_.getValuesMap[Any](List("name", "age"))).collect().foreach(println)
    // 返回结果: Map("name" -> "Justin", "age" -> 19)
    
    sc.stop
  }

  // 定义一个case class.
  // 注意：Scala 2.10的case class最多支持22个字段，要绕过这一限制，
  // 你可以使用自定义class，并实现Product接口。当然，你也可以改用编程方式定义schema
  case class Person(name: String, age: Int)
}