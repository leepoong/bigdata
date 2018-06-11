package com.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class Udf2 extends UDF {
    public IntWritable evaluate(Text t ) {
        return new IntWritable(t.toString().length());
    }
}
