package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * 需要添加JDBC driver访问数据库对应的jar包到Client 、Driver、worker节点
 * SPARK_CLASSPATH=postgresql-9.3-1102-jdbc41.jar bin/spark-shell
 *
 */
object TableCacheSparkSQL {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("TableCacheSparkSQL") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // 设置缓存参数
    //如果设置为true，Spark SQL将会根据数据统计信息，自动为每一列选择单独的压缩编码方式。
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.compressed", "true")
    // 控制列式缓存批量的大小。增大批量大小可以提高内存利用率和压缩率，但同时也会带来OOM（Out Of Memory）的风险。
    sqlContext.setConf("spark.sql.inMemoryColumnarStorage.batchSize", "10000")

    // 从parquet加载数据创建一个 DataFrame
    val df = sqlContext.read.load("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/users.parquet")
    df.registerTempTable("person")

    // 缓存表
    sqlContext.cacheTable("person")
    //或者使用DataFrame进行缓存
    df.cache()

    // 删除缓存
    sqlContext.uncacheTable("person")

    sc.stop()
  }
}