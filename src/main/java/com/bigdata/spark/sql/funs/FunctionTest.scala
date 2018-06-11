package com.bigdata.spark.sql.funs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object FunctionTest {
  case class user(name:String)
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("test").setMaster("local"))
    val sql = new SQLContext(sc)
    val data = Array(
      "20180520,1220,15,32",
      "20180521,1221,23",
      "20180522,1223,25",
      "20180521,1223,53",
      "20180520,1221,43",
      "20180521,1223,65",
      "20180520,1221,54",
      "20180522,1222,12"
    )
    /*val rowRdd: RDD[Row] = sc.parallelize(datas).map(p => {
      val v1 = p.split(",")
      Row(v1(0), v1(1))
    })
    val schema = StructType(Array(
      StructField("date", DataTypes.StringType, false) ,
      StructField("uid", DataTypes.StringType, false)
      )
    )
    val df1 = sql.createDataFrame(rowRdd,schema)
    val df2 = df1.groupBy("date")
      .agg(Map("uid"-> "disdinct" ,"uid"-> "count"))
    df2.show()*/
    val rdd1 = sc.parallelize(data).filter(log => if (log.split(",").length == 3) true else false).map(v => {
      Row(v.split(",")(0), v.split(",")(1).toInt, v.split(",")(2).toInt)
    })
    val schema = StructType(Array(StructField("date",DataTypes.StringType,nullable = false), StructField("uid", DataTypes.IntegerType, nullable = false), StructField("age", DataTypes.IntegerType, nullable = false)))
    val df1 = sql.createDataFrame(rdd1,schema)
    df1.show()
    val df2 = df1.groupBy("date").agg(Map("uid"->"avg","age"->"sum"))
    df2.show
    df2.map(row => Row(row.getString(0))).collect().foreach(println)
  }
}
