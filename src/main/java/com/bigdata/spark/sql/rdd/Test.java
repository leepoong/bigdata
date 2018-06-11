package com.bigdata.spark.sql.rdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Test {

    public static void main(String[] args) {
//        fun1();
//        fun2();
        fun3();
    }

    private static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
        return new JavaSparkContext(conf);
    }

    private static SQLContext getSqlContext() {
        JavaSparkContext sc = getSparkContext();
        return new SQLContext(sc);
    }

    public static void fun1() {
        JavaSparkContext sc = getSparkContext();
        SQLContext sql = new SQLContext(sc);
        JavaRDD<String> rdd1 = sc.textFile("/data/input/student.txt");
        JavaRDD<Student> rdd2 = rdd1.map(new Function<String, Student>() {
            @Override
            public Student call(String v1) throws Exception {
                String[] vs = v1.split(",");
                int id = Integer.valueOf(vs[0]);
                String name = vs[1];
                int age = Integer.valueOf(vs[2]);
                String gender = vs[3];
                return new Student(name, gender, age, id);
            }
        });
        DataFrame dataFrame = sql.createDataFrame(rdd2, Student.class);
        dataFrame.show();
    }

    public static void fun2() {
        JavaSparkContext sc = getSparkContext();
        SQLContext sql = new SQLContext(sc);
        JavaRDD<String> rdd1 = sc.textFile("/data/input/student.txt");
        JavaRDD<Row> rdd2 = rdd1.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                String[] vs = v1.split(",");
                String name = vs[1];
                int id = Integer.valueOf(vs[0]);
                String gender = vs[3];
                int age = Integer.valueOf(vs[2]);
                return RowFactory.create(id, name, age, gender);
            }
        });
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, false),
                DataTypes.createStructField("gender", DataTypes.StringType, false),
        });
        DataFrame df = sql.createDataFrame(rdd2, schema);
        df.registerTempTable("student");
        DataFrame df2 = sql.sql("select * from student where age < 40");
        List<Row> rows = df2.javaRDD().collect();
        for (Row row : rows) {
            System.out.println(row.get(1));
            System.out.println(row.get(2));
        }
    }

    /**
    @author Lee Pong
    @date   2018/6/4 8:21
    @return void
    */
    public static void fun3() {
        JavaSparkContext sc = getSparkContext();
        SQLContext sql = new SQLContext(sc);

        //构造学生信息表
        DataFrame df = sql.read().json("/data/input/student.json");
        df.registerTempTable("stu_info");
        df.show();

        //构造学生成绩表
        List<String> list = new ArrayList<>();
        list.add("zhangsan,89");
        list.add("lisi,95");
        list.add("wangerma,59");
        list.add("sanshang,56");
        list.add("haiyi,89");
        JavaRDD<String> rdd1 = sc.parallelize(list);
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("score", DataTypes.IntegerType, true)
        });
        JavaRDD<Row> rdd2 = rdd1.map(new Function<String, Row>() {
            @Override
            public Row call(String v1) throws Exception {
                return RowFactory.create(v1.split(",")[0], Integer.valueOf(v1.split(",")[1]));
            }
        });
        DataFrame df2 = sql.createDataFrame(rdd2, schema);
        df2.registerTempTable("stu_grade");

        //获取成绩大于60的学生成绩表
        DataFrame df3 = sql.sql("select * from stu_grade where score > 60");
        df3.show();

        List<String> list2 = df3.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.getString(0);
            }
        }).collect();
        StringBuilder sqlText = new StringBuilder("select * from stu_info where name in (");
        for (int i = 0; i < list2.size(); i++) {
            sqlText.append("'").append(list2.get(i)).append("'");
            if (i != list2.size() - 1) {
                sqlText.append(",");
            }
        }
        sqlText.append(")");
        DataFrame df4 = sql.sql(sqlText.toString());
        df4.show();


        JavaPairRDD<String, Integer> pair1 = df3.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<>(row.getString(0), row.getInt(1));
            }
        });

        JavaPairRDD<String, Integer> pair2 = df4.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                //row.getLong这里需要注意
                return new Tuple2<>(row.getString(1), (int) row.getLong(0));
            }
        });

        /*pair2.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1);
            }
        });*/

        JavaPairRDD<String, Tuple2<Integer, Integer>> pair3 = pair1.join(pair2);
        /*pair3.foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<String, Tuple2<Integer, Integer>> vs) throws Exception {
                System.out.println(vs._1);
                System.out.println(vs._2._1);
                System.out.println(vs._2._2);
                System.out.println("==================================");
            }
        });*/

        StructType schema2 = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("score", DataTypes.IntegerType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
        });

        JavaRDD<Row> rdd5 = pair3.map(new Function<Tuple2<String, Tuple2<Integer, Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> v1) throws Exception {
                return RowFactory.create(v1._1, v1._2._1, v1._2._2);
            }
        });

        DataFrame df5 = sql.createDataFrame(rdd5, schema2);
        df5.show();


        /*
        *                   大功告成！！！！！！！！
        * */


    }

}
