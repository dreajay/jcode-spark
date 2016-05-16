package com.jcode.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SaveMode

/**
 * 需要添加JDBC driver访问数据库对应的jar包到Client 、Driver、worker节点
 * SPARK_CLASSPATH=postgresql-9.3-1102-jdbc41.jar bin/spark-shell
 *
 */
object JdbcSparkSQL {
  def main(args: Array[String]): Unit = {
    // 创建SparkConf
    val conf = new SparkConf() //
      .setAppName("JdbcSparkSQL") //
      .setMaster("local[2]")
      .setJars(List("C:\\Users\\daijunjie\\.m2\\repository\\mysql\\mysql-connector-java\\5.1.38\\mysql-connector-java-5.1.38.jar"))

    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    //    属性名	                     含义
    //    url	                         需要连接的JDBC URL
    //    dbtable	              需要读取的JDBC表。注意，任何可以填在SQL的where子句中的东西，都可以填在这里。（既可以填完整的表名，也可填括号括起来的子查询语句）
    //    driver	     JDBC driver的类名。这个类必须在master和worker节点上都可用，这样各个节点才能将driver注册到JDBC的子系统中。
    //    partitionColumn, lowerBound, upperBound, numPartitions	这几个选项，如果指定其中一个，则必须全部指定。他们描述了多个worker如何并行的读入数据，并将表分区。partitionColumn必须是所查询的表中的一个数值字段。注意，lowerBound和upperBound只是用于决定分区跨度的，而不是过滤表中的行。因此，表中所有的行都会被分区然后返回。
    //    fetchSize	   JDBC fetch size，决定每次获取多少行数据。在JDBC驱动上设成较小的值有利于性能优化（如，Oracle上设为10）

    val df = sqlContext.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://127.0.0.1:3306/test", // JDBC URL
        "dbtable" -> "user", // JDBC表。任何可以填在SQL的where子句中的东西，都可以填在这里。（既可以填完整的表名，也可填括号括起来的子查询语句）
        "driver" -> "com.mysql.jdbc.Driver", //JDBC driver的类名
        "user" -> "root", //JDBC driver的类名
        "password" -> "root" //JDBC driver的类名
        ))
      .load()

    // 打印数据树形结构
    df.printSchema()
    // 展示 DataFrame 的内容
    df.show()
    // 查找
    df.select("userName").show()

    sc.stop()
  }
}