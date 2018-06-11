package com.bigdata.spark.sql.jdbc;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;

public class SparkToJdbc {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local"));
        SQLContext sql = new SQLContext(sc);
        JavaRDD<Row> rdd = sc.textFile("2015082818").map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] strings = v1.split("\t");
                return RowFactory.create(strings);
            }
        });
        /*rdd.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.length());
                for (int i = 0; i < row.length(); i++) {
                    System.out.println(row.get(i));
                }
            }
        });*/
        Row row = rdd.first();
        for (int i = 0; i < row.length(); i++) {
            String str = row.getString(i);
            if (str == null) {
                System.out.println("hello");
            } else {
                System.out.println(str);
            }
        }
        /*Map<String, String> maps = new HashMap<>();
        maps.put("url", "jdbc:mysql://localhost:3306");
        maps.put("dbtable", "school.student");
        maps.put("driver", "com.mysql.jdbc.Driver");
        maps.put("user", "root");
        maps.put("password", "lipeng2zhangmin");
        DataFrame df = sql.read().format("jdbc").options(maps).load();
        df.show();
        df.registerTempTable("temp");
        String sqlText = "select * from temp where age < 30";
        DataFrame df2 = sql.sql(sqlText);
        List<Row> rows = df2.javaRDD().collect();
        for (Row row : rows) {
            System.out.println(row.getString(0)+"   "+row.getInt(1));
        }
        df2.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String name = row.getString(0);
                int age = row.getInt(1);
                String sqlText = "insert into person values('"+name +"' ,"+ age + ")";
                Class.forName("com.mysql.jdbc.Driver");
                Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/school", "root", "lipeng2zhangmin");
                PreparedStatement ps = conn.prepareStatement(sqlText);
                ps.execute();
            }
        });*/
    }
}
