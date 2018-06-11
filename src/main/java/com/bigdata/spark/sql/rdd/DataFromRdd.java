package com.bigdata.spark.sql.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
/*
* 两个DataFrame 如何联表查询？
* 先转换成 JavaPairRdd 然后通过 join 操作
* 最后再转换成 DataFrame 查询
* */

public class DataFromRdd {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("test").setMaster("local"));
        SQLContext sql = new SQLContext(sc);

        /*
        * DataFrame由 data 和 Schema 组成
        * 创建DataFrame两种方式 ： JavaRdd<Row> & StructType 或者 JavaRdd<?> & JavaBean
        * */
        JavaRDD<Row> rdd1 = sc.textFile("student.txt").map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] strings = v1.split(",");
                //RowFactory获取Row
                return RowFactory.create(strings[0], Integer.parseInt(strings[1]), Integer.parseInt(strings[2]));
            }
        });
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType,false) ,
                DataTypes.createStructField("id", DataTypes.IntegerType,false),
                DataTypes.createStructField("age",DataTypes.IntegerType,false)
        });
        DataFrame df1 = sql.createDataFrame(rdd1, schema);
        df1.show();
        df1.registerTempTable("temp1");
        DataFrame df2 = sql.sql("select name ,id  from temp1 where id < 3 ");
        df2.show();
        // 将 df2 转换成 JavaPairRdd 操作
        JavaPairRDD<String,Integer> pair1 = df2.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        });
        /*pair1.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1+"  : "+stringIntegerTuple2._2);
            }
        });*/

        /*
        * 第二种方式创建 DataFrame , 通过JavaBean
        * */
        List<String> list = new ArrayList<>(Arrays.asList(
                "zhangsan,male",
                "lisi,male",
                "wangerma,male",
                "lipeng,female"
        ));
        JavaRDD<Student> rdd2 = sc.parallelize(list).map(new Function<String, Student>() {
            @Override
            public Student call(String v1) throws Exception {
                String[] strings = v1.split(",");
                Student s = new Student();
                s.setName(strings[0]);
                s.setGender(strings[1]);
                return s;
            }
        });
        DataFrame df3 = sql.createDataFrame(rdd2, Student.class);
        df3.show();
        df3.registerTempTable("temp2");
        String sqlStatement = "select * from temp2 where name in (";
        Row[] rows = df2.collect();
        for (int i = 0; i < rows.length ; i++) {
            sqlStatement += "\'" + rows[i].getString(0) + "\'";
            if (i != rows.length-1) {
                sqlStatement += ",";
            }
        }
        sqlStatement += ")";
        DataFrame df4 = sql.sql(sqlStatement);
        df4.show();
        JavaPairRDD<String, String> pair2 = df4.javaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(1), row.getString(0));
            }
        });
        JavaPairRDD<String , Tuple2<Integer,String>> rdd5 = pair1.join(pair2);
        rdd5.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, String>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, String>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2._1+" id : "+stringTuple2Tuple2._2._1+" gender : "+stringTuple2Tuple2._2._2);
            }
        });
    }
}
