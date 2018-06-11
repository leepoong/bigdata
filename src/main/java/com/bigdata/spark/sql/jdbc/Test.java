package com.bigdata.spark.sql.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

public class Test {
    public static void main(String[] args) {
//        fun1();
        fun2();

    }

    public static SQLContext getSqlContext() {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        return new SQLContext(sc);
    }

    public static void fun1() {
        SQLContext sqlContext = getSqlContext();
        Map<String, String> map = new HashMap<>();
        map.put("url", "jdbc:mysql://leepong1:3306/school");
        map.put("dbtable", "student");
        map.put("user", "root");
        map.put("password", "318277");
        DataFrame df = sqlContext.read().format("jdbc").options(map).load();
        df.registerTempTable("student");
        DataFrame df2 = sqlContext.sql("select * from student where name in ( 'lisi' , 'wangerma')");
        df2.show();
        df.printSchema();
    }

    public static void fun2() {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("test"));
        HiveContext hiveContext = new HiveContext(sc.sc());
        DataFrame df = hiveContext.sql("select * from school.student");
        df.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String name = row.getString(0);
                int age = Integer.valueOf(row.getString(1));
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = DriverManager.getConnection("jdbc:mysql://leepong1:3306/school", "root", "318277");
                String sql = "insert into student values ( '" + name + "' , " + age + ")";
                PreparedStatement ps = conn.prepareStatement(sql);
                boolean result = ps.execute();
                System.out.println(result);
            }
        });
        sc.close();
    }

}
