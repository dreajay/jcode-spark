package com.jcode.spark.ml

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Arrays

object MovieML {

  def main(args: Array[String]) = {

    val master = "local[2]"

    val conf = new SparkConf().setAppName("Spark ML Test")
      .setMaster(master)
      .setJars(List("D:\\study\\jcode\\jcode-spark\\target\\jcode-spark-0.0.1-SNAPSHOT.jar"))
    //调试

    val sc = new SparkContext(conf)

    //用户数据
    val user_data = sc.textFile("E:/temp/ml-100k/u.user")
    println("user data:" + user_data.first())
    val user_fields = user_data.map(_.split("\\|"))

    val num_users = user_fields.map(_(0)).count()
    val num_ages = user_fields.map(_(1)).distinct().count()
    val num_genders = user_fields.map(_(2).trim()).distinct().count()

    val num_occupations = user_fields.map(_(3)).distinct().count()

    val num_zipcodes = user_fields.map(_(4)).distinct().count()

    println("Users: " + num_users)
    println("ages: " + num_ages)
    println("genders: " + num_genders)
    println("occupations: " + num_occupations)
    println("ZIP codes: " + num_zipcodes)

    //电影数据
    val movie_data = sc.textFile("E:/temp/ml-100k/u.item")
    println("movie data:" + movie_data.first())
    val num_movies = movie_data.count()
    println("Movies: " + num_movies)

    val movie_fields = movie_data.map(_.split("\\|"))
    val years = movie_fields.map(_(2).split("\\-")).map(_(2)).map(convert_year(_))
    //解析出错的数据的年份已设为1900。要过滤掉这些数据可以使用Spark的filter转换操作:
    
    println(Arrays.toString(years.take(10)))
    val years_filtered = years.filter(_ != 1900)
    println(Arrays.toString(years_filtered.take(20)))

    val movie_ages = years_filtered.map(2006-_)
    .map(word =>(word,1))
    .reduceByKey(_+_)
        println(movie_ages.take(20))
//    .countByValue()
    
//    println(movie_ages.seq.toString())
  }

  def convert_year(x: String): Int = {
    try {
      x.toInt
    } catch {
      case t: Throwable => 1900
    }
  }

}  