package com.jcode.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SimpleApp {

  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("Spark Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(args(0), 2).cache
    val numAs = logData.filter(line => line.contains("a")).count
    val numBs = logData.filter(line => line.contains("b")).count
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }

}  