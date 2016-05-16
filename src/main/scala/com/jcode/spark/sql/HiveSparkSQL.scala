package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 *
 */
object HiveSparkSQL {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("HiveSparkSQL") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)

    // 创建HiveContext对象
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

    // 设置Hive属性
    //    sqlContext.setConf("spark.sql.parquet.cacheMetadata", "true");

    // 创建表
    sqlContext.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    //导入数据
    sqlContext.sql("LOAD DATA LOCAL INPATH 'E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/kv1.txt' INTO TABLE src")

    // 使用HiveQL查询
    val df = sqlContext.sql("FROM src SELECT key, value")

    df.collect().foreach(println)

    //   保存到hive表
    df.write.mode(SaveMode.Append).saveAsTable("hivekvtable")
    
    //   从hive表创建一个 DataFrame,然后可以进行修改查询
    val sqlDF = sqlContext.sql("SELECT * FROM hivekvtable")

    // 之后可以启动命令行 bin/spark-sql执行SELECT * FROM hivekvtable查看表数据

    sc.stop()
  }
}