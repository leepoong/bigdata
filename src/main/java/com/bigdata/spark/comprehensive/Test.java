package com.bigdata.spark.comprehensive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.Tuple2;

import java.util.*;

public class Test {
    public static void main(String[] args) {
        final JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local"));
        SQLContext sql = new SQLContext(sc);
        JavaRDD<String> rdd1 = sc.textFile("girls.txt");
        final Map<String, List<String>> map = new HashMap<>();
        map.put("city", Arrays.asList("beijing", "tianjin", "shanghai"));
        map.put("os", Arrays.asList("android"));
        map.put("version", Arrays.asList("2.0", "3.0"));
        final Broadcast<Map<String,List<String>>> broadcast = sc.broadcast(map);
        JavaRDD<String> rdd2 = rdd1.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                String[] strings = v1.split("\t");
                String city = strings[3];
                String os = strings[4];
                String version = strings[5];
                Map<String,List<String>> map = broadcast.value();
                if (!map.get("city").contains(city)) {
                    return false;
                }
                if (!map.get("os").contains(os)) {
                    return false;
                }
                if (!map.get("version").contains(version)) {
                    return false;
                }
                return true;
            }
        });
        rdd2.foreach(new VoidFunction<String>() {
            int num = 0;
            @Override
            public void call(String s) throws Exception {
                num += 1;
                System.out.println(s);
                System.out.println(num);
            }
        });
        JavaPairRDD<String,String> pairRDD = rdd2.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String s) throws Exception {
                String[] vs = s.split("\t");
                String date = vs[0];
                String word = vs[2];
                String name = vs[1];
                return new Tuple2<>(date + "_" + word, name);
            }
        });
        JavaPairRDD<String,Iterable<String>> javaPairRDD = pairRDD.groupByKey();
        JavaPairRDD<String,Long> v1= javaPairRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                String keyWord = stringIterableTuple2._1;
                Iterator<String> iterator = stringIterableTuple2._2.iterator();
                List<String> users = new ArrayList<>();
                while (iterator.hasNext()) {
                    String user = iterator.next();
                    if (!users.contains(user)) {
                        users.add(user);
                    }
                }
                return new Tuple2<>(keyWord, (long) users.size());
            }
        });
        /*v1.foreach(new VoidFunction<Tuple2<String, Long>>() {
            @Override
            public void call(Tuple2<String, Long> stringLongTuple2) throws Exception {
                System.out.println(stringLongTuple2._1+stringLongTuple2._2);
            }
        });
        */
        JavaRDD<Row> rowJavaRDD = v1.map(new Function<Tuple2<String, Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> v1) throws Exception {
                String date = v1._1.split("_")[0];
                String word = v1._1.split("_")[1];
                long num = v1._2;
                return RowFactory.create(date, word, num);
            }
        });
        List<StructField> types = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType,true),
                DataTypes.createStructField("word", DataTypes.StringType,true),
                DataTypes.createStructField("num", DataTypes.LongType,true)
        );
        DataFrame df = sql.createDataFrame(rowJavaRDD, DataTypes.createStructType(types));
        df.registerTempTable("temp");
        DataFrame df2 = sql.sql("select * from (" +
                " select date , word , num ," +
                " row_number() over) partition by date order by num ) as rank " +
                "from temp ) temp1");
        df2.show();
    }
}
