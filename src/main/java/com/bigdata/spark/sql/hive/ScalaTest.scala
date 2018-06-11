package com.bigdata.spark.sql.hive

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object ScalaTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    val data = sql.read.json("/data/input/student.json")
    data.show()
    data.printSchema()
    data.select("name").show()
  }
}
