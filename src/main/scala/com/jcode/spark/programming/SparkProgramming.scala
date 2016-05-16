package com.jcode.spark.programming

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * 运行：
 * $ ./bin/spark-shell --master local[4] --jars code.jar
 */
object SparkProgramming {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("SparkProgramming") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    //利用集合创建分布式RDD
    val data = Array(1, 2, 3, 4, 5)
    val distRDD = sc.parallelize(data, 2)
    // 计算和
    val sum = distRDD.reduce((a, b) => a + b)
    println("sum=" + sum)

    /**
     * 利用外部数据源（本地文件系统、HDFS、Cassandra、HBase、Amazon S3）创建分布式RDD
     * Spark 支持的文件格式包括：文本文件（text files）、SequenceFiles，以及其他 Hadoop 支持的输入格式（InputFormat）
     */
    /**
     * textFile传入一个路径参数，没有前缀，默认为HDFS
     * 本地文件：Linux：file://,Win:E:/
     * Hdfs : hdfs://，
     * S3:s3n://
     * textFile支持目录，压缩文件，以及通配符，比如：
     * textFile("/my/directory”), textFile("/my/directory/\\*.txt”), 以及 textFile("/my/directory/\\*.gz”)
     */
    val distFile = sc.textFile("E:/temp/sparkprogramming.txt");
    // 计算文本行的长度，然后计算长度总和
    val fileLength = distFile.map(line => line.length())
    // 计算长度总和
    .reduce((a, b) => a + b)
    println("dist file length:" + fileLength)

    //输出到本地,
    distFile.saveAsTextFile("E:/temp/sparkprogrammingOut/")

    sc.stop()
  }

}