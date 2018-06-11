package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DataFromHDFSS {
  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("word_count").setMaster("local[2]")
    val ssc = new StreamingContext(sc,Seconds(5))
    val ds1 = ssc.textFileStream("hdfs://leepong1:8020")
    val ds2: DStream[String] = ds1.flatMap(p => p.split(","))
    val ds3: DStream[(String, Int)] = ds2.map(v => (v,1)).reduceByKey((v1, v2)=>v1+v2)
    ds3.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
