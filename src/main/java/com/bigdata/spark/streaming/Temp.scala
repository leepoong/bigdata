package com.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.streaming.api.java.{JavaDStream, JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object Temp {
  def main(args: Array[String]): Unit = {
    /*val ssc = new StreamingContext(new SparkConf().setMaster("local[2]").setAppName("test"),Seconds(1))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost",9998)
    val rs: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map(v=>(v,1)).reduceByKey(_+_)
    rs.print()
    ssc.start()
    ssc.awaitTermination()*/

  }

}
