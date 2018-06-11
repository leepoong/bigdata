package com.bigdata.spark.core;

import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("test");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> rdd1 = sc.textFile("file://D://dev//tools//jetbrains//apps//IDEA-U//projects//spark//temp.txt").persist(StorageLevel.DISK_ONLY());
        JavaPairRDD<String, Integer> pair1 = rdd1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s.split(",")[0], Integer.valueOf(s.split(",")[1]));
            }
        });
        JavaPairRDD<String, Iterable<Integer>> pair2 = pair1.groupByKey();
        JavaPairRDD<String, Iterable<Integer>> pair4 = pair2.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                List<Integer> list = new ArrayList<>();
                List<Integer> list2 = IteratorUtils.toList(tuple2._2.iterator());
                Collections.sort(list2);
                Collections.reverse(list2);
                List<Integer> list3 = list2.subList(0, 3);
                return new Tuple2<String, Iterable<Integer>>(tuple2._1, list3);
            }
        });
        pair4.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple2) throws Exception {
                System.out.println(tuple2._1);
                System.out.println(tuple2._2);
                System.out.println("=============================");
            }
        });
    }

}
