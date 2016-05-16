package com.jcode.spark.programming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Foreach {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("Foreach") //
//      .setMaster("local[2]")
//      .setMaster("spark://node70:7077")
//      .setJars(List("D:\\study\\jcode\\jcode-spark\\target\\jcode-spark-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //利用集合创建分布式RDD
    val data = Array(1, 2, 3, 4, 5)
    val distRDD = sc.parallelize(data, 2)
    // 
    /**
     * 千万不要做，这样是错误的，在本地模式Worker和Driver运行在同一个JVM，计算可能正确，
     * 在分布式情况下，每个worker都有自己的counter副本，Driver的counter变量对于worker来说是不可见的，
     * 这种计算应该使用累加器Accumulator共享变量进行计算
     */
    var counter: Int = 0
    distRDD.foreach(x => counter += x)
    println("Counter value: " + counter)

    //下面这两种单机情况可以正常打印，但是集群情况，这种打印都会打印在worker节点，Driver上不会打印
    distRDD.foreach(println)
    distRDD.map(println)

    // 如果需要打印，先通过collect Action算子把RDD拉到Driver节点，然后打印
    distRDD.collect().foreach(println)
    // 如果数据量太多，打印所有数据可能会导致Driver节点内存溢出，可以使用take Action算子只打印一部分
    distRDD.take(2).foreach(println)
    
    sc.stop()
  }
}