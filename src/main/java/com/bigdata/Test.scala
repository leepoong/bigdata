package com.bigdata

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("file://D://dev//tools//jetbrains//apps//IDEA-U//projects//spark//teacher.txt")
    val rdd2 = rdd1.flatMap(v=>v.split(" ")).map(v=>(v,1))
    val v1: RDD[(String, Iterable[Int])] = rdd2.groupByKey()
    v1.foreach(v=>{
      println(v._2.foreach(println))
    })
  }

}
