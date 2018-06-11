package com.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
/*
* 自定义hive的转换大小写udf函数
* 定义传入值0为转换为小写字符串，1为大写字符串，默认转换为小写字符串
* */
public class LeeUdf extends UDF {
    public Text evaluate(Text str) {
        return this.evaluate(str, new IntWritable(0));
    }

    public Text evaluate(Text str , IntWritable num) {
        if (str != null) {
            if (num.get() == 0) {
                return new Text(str.toString().toLowerCase());
            } else if (num.get() == 1) {
                return new Text(str.toString().toUpperCase());
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

}
