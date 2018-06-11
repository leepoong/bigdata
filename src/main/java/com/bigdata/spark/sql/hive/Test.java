package com.bigdata.spark.sql.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class Test {
    public static void main(String[] args) {
        fun1();
    }

    public static JavaSparkContext getSparkContext() {
        return new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local"));
    }

    public static void fun1() {
        JavaSparkContext sc = getSparkContext();
        HiveContext hive = new HiveContext(sc.sc());
        DataFrame df1 = hive.sql("select * from school.student");
        df1.show();

    }
}
