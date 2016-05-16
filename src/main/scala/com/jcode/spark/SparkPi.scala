package com.jcode.spark

import scala.math.random

import org.apache.spark._

import org.apache.spark.SparkConf

object SparkPi {
  def main(args: Array[String]) {
    val master = "spark://node70:7077"
    //        val master = "local"

    val conf = new SparkConf().setAppName("Spark Pi Test")
      .setMaster(master)
      .setJars(List("D:\\study\\jcode\\jcode-spark\\target\\jcode-spark-0.0.1-SNAPSHOT.jar"))
    //调试
    //      .set("spark.executor.extraJavaOptions", "-Xdebug -Xrunjdwp:transport=dt_socket,address=8005,server=y,suspend=n")

    //    println("sleep begin.")
    //    Thread.sleep(10000) //等待10s，让有足够时间启动driver的remote debug
    //    println("sleep end.")

    val spark = new SparkContext(conf)
    val slices = if (args.length > 0) args(0).toInt else 2
    val n = 100000 * slices
    val count = spark.parallelize(1 to n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y < 1) 1 else 0
    }.reduce(_ + _)
    println("Pi is roughly " + 4.0 * count / n)
    spark.stop()
  }
}
