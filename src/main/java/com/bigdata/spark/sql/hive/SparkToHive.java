package com.bigdata.spark.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

import java.util.HashMap;


public class SparkToHive {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("test"));
        HiveContext hiveSql = new HiveContext(sc.sc());
        hiveSql.sql("use school ");
        DataFrame df = hiveSql.sql("select * from student ");
        DataFrame df1 = df.groupBy("gender").agg(new HashMap<String, String>(){
            {
                put("gender", "count");
            }
        });
        df1.show();
    }
}
