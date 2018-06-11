package com.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class DataFromHDFS {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf()
                .setMaster("local[2]")
                .setAppName("WordCount");
        JavaStreamingContext jsc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaDStream<String> ds = jsc.textFileStream("hdfs://leepong1:8020/streaming_data");
        JavaDStream<String> ds2 = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });
        JavaPairDStream<String,Integer> pairDStream = ds2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String,Integer> pairDStream1 = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        pairDStream1.print();

        jsc.start();
        jsc.awaitTermination();
        jsc.close();
    }
}
