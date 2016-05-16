package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

object MergeMultiSchema {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("MergeMultiSchema") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // 继续沿用之前的sqlContext对象
    // 为了支持RDD隐式转换为DataFrame
    import sqlContext.implicits._

    // 创建一个简单的DataFrame，存到一个分区目录中
    val df1 = sc.makeRDD(1 to 5).map(i => (i, i * 2)).toDF("single", "double")
    df1.write.parquet("data/test_table/key=1")

    // 创建另一个DataFrame放到新的分区目录中，
    // 并增加一个新字段，丢弃一个老字段
    val df2 = sc.makeRDD(6 to 10).map(i => (i, i * 3)).toDF("single", "triple")
    df2.write.parquet("data/test_table/key=2")

    // 读取分区表
    val df3 = sqlContext.read.option("mergeSchema", "true").parquet("data/test_table")
   
    df3.printSchema()
    // 最终的schema将由3个字段组成（single，double，triple）
    // 并且分区键出现在目录路径中
    // root
    // |-- single: int (nullable = true)
    // |-- double: int (nullable = true)
    // |-- triple: int (nullable = true)
    // |-- key : int (nullable = true)

    sc.stop()
  }
}