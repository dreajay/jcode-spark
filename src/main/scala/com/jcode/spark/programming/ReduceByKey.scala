package com.jcode.spark.programming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 键值对（key-value pair）RDD常见于做分布式混洗（shuffle）操作，如：以key分组或聚合
 * reduceByKey算子是其中一种
 */
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("Foreach") //
      .setMaster("local[2]")
//      .setMaster("spark://node70:7077")
//      .setJars(List("D:\\study\\jcode\\jcode-spark\\target\\jcode-spark-0.0.1-SNAPSHOT.jar"))

    val sc = new SparkContext(conf)

    //利用集合创建分布式RDD
    val data = Array(1, 2, 2, 3, 3, 3)
    val distRDD = sc.parallelize(data, 2)
    // 计算每个元素出现的次数(1,1)、(2,1)、(2,1)、(3,1)、(3,1)、(3,1)
    val pairs = distRDD.map(n => (n,1))
    // 对每个元素key出现的次数进行相加(2,1)、(2,1) => (2,2)
    val counts = pairs.reduceByKey((a, b) => a + b)
    // 对键值对进行排序
    val sort = counts.sortByKey()
    
    // 返回数据给Driver节点打印
    counts.collect().foreach(println)
    sort.collect().foreach(println)
    
    sc.stop()
  }
}