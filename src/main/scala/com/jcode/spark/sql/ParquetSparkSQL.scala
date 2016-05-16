package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
 */
object ParquetSparkSQL {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("ParquetSparkSQL") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

     // 从parquet加载数据创建一个 DataFrame
    val df = sqlContext.read.load("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/users.parquet")
   
    
    // 输出数据到parquet文件, SaveMode 保存模式
    df.select("name", "favorite_color").write.mode(SaveMode.Overwrite).save("namesAndFavColors.parquet")
    

    //Spark SQL还支持直接对文件使用SQL查询，不需要用read方法把文件加载进来。
    val dirtDF = sqlContext.sql("SELECT * FROM parquet.`E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/users.parquet`")

    

    // 展示 DataFrame 的内容
    df.show()
    //+------+--------------+----------------+
    //|  name|favorite_color|favorite_numbers|
    //+------+--------------+----------------+
    //|Alyssa|          null|  [3, 9, 15, 20]|
    //|   Ben|           red|              []|
    //+------+--------------+----------------+

    // 打印数据树形结构
    df.printSchema()
    //root
    // |-- name: string (nullable = false)
    // |-- favorite_color: string (nullable = true)
    // |-- favorite_numbers: array (nullable = false)
    // |    |-- element: integer (containsNull = false)

    
    
    sc.stop()
  }
}