package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.DataFrame
 */
object DataFrameAction {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("DataFrameAPI") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    // 创建一个 DataFrame
    val df = sqlContext.read.json("E:/soft/spark-1.6.0-bin-hadoop2.6/examples/src/main/resources/people.json")

    // 展示 DataFrame 的内容
    df.show()
    // age  name
    // null Michael
    // 30   Andy
    // 19   Justin

    // 打印数据树形结构
    df.printSchema()
    // root
    // |-- age: long (nullable = true)
    // |-- name: string (nullable = true)

    // select "name" 字段
    df.select("name").show()
    // name
    // Michael
    // Andy
    // Justin

    // 展示所有人，但所有人的 age 都加1
    df.select(df("name"), df("age") + 1).show()
    // name    (age + 1)
    // Michael null
    // Andy    31
    // Justin  20

    // 筛选出年龄大于21的人
    df.filter(df("age") > 21).show()
    // age name
    // 30  Andy

    // 计算各个年龄的人数
    df.groupBy("age").count().show()
    // age  count
    // null 1
    // 19   1
    // 30   1

    //注册到临时表
    df.registerTempTable("people")

    //查询
    val peoples = sqlContext.sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
    peoples.map(t => "Name: " + t(0)).collect().foreach(println)
    // Name: Justin
    
    
     // 输出数据到parquet文件, SaveMode 保存模式
    df.select("name", "age").write.mode(SaveMode.Overwrite).save("namesAndAge.parquet")
    
    
    sc.stop()
  }
}