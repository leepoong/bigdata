package com.bigdata.spark.sql.json;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkToJson {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local"));
        SQLContext sql = new SQLContext(sc);

        /*
        * json数据源可以是json文件，或者是JavaRdd<String>
        * */
        /*DataFrame df1 = sql.read().json("student.json");
        df1.show();*/
        List<String> list = new ArrayList<>(Arrays.asList(
                "{\"name\":\"zhangsan\",\"age\":25}",
                "{\"name\":\"lisi\",\"age\":26}",
                "{\"name\":\"wangerma\",\"age\":24}"
        ));
        JavaRDD<String> jsonRdd = sc.parallelize(list);
        DataFrame df2 = sql.read().json(jsonRdd);
        df2.show();
    }
}
