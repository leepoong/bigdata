package com.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class Test {
    public static void main(String[] args) {
//        fun1();
        fun2();
    }

    public static void fun1() {
        JavaStreamingContext jsc = new JavaStreamingContext(new SparkConf().setMaster("local[2]").setAppName("test"), Durations.seconds(5));
        JavaReceiverInputDStream<String> in = jsc.socketTextStream("localhost", 9999);
        JavaDStream<String> dStream = in.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });
        JavaPairDStream<String, Integer> pairDStream = dStream.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> pairDStream1 = pairDStream.reduceByKey(new Function2<Integer, Integer, Integer>() {
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

    public static void fun2() {
        JavaStreamingContext sc = new JavaStreamingContext(new SparkConf().setAppName("test").setMaster("local[2]"), Durations.seconds(10));
        JavaDStream<String> ds = sc.textFileStream("/data/spark");
        JavaDStream<String> ds2 = ds.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairDStream<String, Integer> pair = ds2.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> pair2 = pair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        pair2.print();
        sc.start();
        sc.awaitTermination();
        sc.close();
    }

}
