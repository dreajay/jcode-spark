package com.jcode.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object WordCount {

  def main(args: Array[String]) {
    if (args.length != 2) {
      println("usage is com.jcode.spark.WordCount <input> <output>")
      return
    }
    val sparkConf = new SparkConf().setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val textFile = sc.textFile(args(0))
    val result = textFile.flatMap(line => line.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    result.saveAsTextFile(args(1))
  }
}