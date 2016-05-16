package com.jcode.spark.programming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MapAndFlatMap {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("SparkProgramming") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    /**
     * test.txt 内容
     * 1 2
		 * 1 2 3
		 * 4 5 6 
		 * 2 3 5
		 * 1 6 3
     */
    val distFile = sc.textFile("E:/temp/test.txt");
    
    // map为每一个元素做处理，返回每一个单独的对象，这里为每一行做空格分隔
    //Array[Array[String]] = Array(Array(1, 2), Array(1, 2, 3), Array(4, 5, 6), Array(2, 3, 5), Array(1, 6, 3))
    val mapResult = distFile.map(line => line.split("\\s+"))
    mapResult.collect().foreach(println)// 打印的是一个Array对象

    // flatMap和Map一样，为每个元素做处理，但是最后返回的是所有对象的一个集合
    //result = Array[String] = Array(1, 2, 1, 2, 3, 4, 5, 6, 2, 3, 5, 1, 6, 3)
    val flatMapResult = distFile.flatMap(line => line.split("\\s+"))
    flatMapResult.collect().foreach(println)// 打印所有元素

    sc.stop()
  }
}