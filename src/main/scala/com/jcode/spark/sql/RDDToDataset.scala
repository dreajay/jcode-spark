package com.jcode.spark.sql
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object DatasetAndRDD {
  def main(args: Array[String]): Unit = {

    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("DatasetAndRDD") //
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 对普通类型数据的Encoder引入import sqlContext.implicits._ 可自动提供
    import sqlContext.implicits._
    val ds = Seq(1, 2, 3).toDS()

    ds.map(_ + 1).collect() // 返回: Array(2, 3, 4)

    // 将RDD转换为Dataset
    val personDS = Seq(Person("Andy", 32)).toDS()
    
    // DataFrame转换为Dataset
    // DataFrame只需提供一个和数据schema对应的class即可转换为 Dataset。Spark会根据字段名进行映射。
    val path = "examples/src/main/resources/people.json"
    val people = sqlContext.read.json(path).as[Person]

    
    
    
    sc.stop()
  }

  // 定义case class，同时也自动为其创建了Encoder
  case class Person(name: String, age: Long)

}