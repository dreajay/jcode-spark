package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

object JsonSparkSQL {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("JsonSparkSQL") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // 从json数据创建一个 DataFrame
    val df = sqlContext.read.json("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/people.json")
    // 指定格式为json创建DataFrame
    val jsondf = sqlContext.read.format("json").load("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/people.json")
    // 另一种方法是，用一个包含JSON字符串的RDD来创建DataFrame
    val anotherPeopleRDD = sc.parallelize("""{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val anotherPeople = sqlContext.read.json(anotherPeopleRDD)

    // 展示 DataFrame 的内容
    df.show()
    // age  name
    // null Michael
    // 30   Andy
    // 19   Justin

    sc.stop()
  }
}