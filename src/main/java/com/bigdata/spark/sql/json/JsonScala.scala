package com.bigdata.spark.sql.json

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object JsonScala {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
    val sql = new SQLContext(sc)
    val df = sql.read.json("student.json")
    df.show()
  }
}
