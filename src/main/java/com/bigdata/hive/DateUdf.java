package com.bigdata.hive;
/**
 * 日期格式转换 当前日期格式：31/Aug/2015:00:04:37 +0800 目标日期格式：2015-08-31 00:04:37
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class DateUdf extends UDF {
    public Text evaluate(Text text) {
        SimpleDateFormat inFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
        SimpleDateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String date = text.toString();
        System.out.println(date);
        if (date == null) {
            return null;
        }
        if (StringUtils.isBlank(date)) {
            return null;
        }
        String outDate = null;
        try {
            Date inDate = inFormat.parse(date);
            outDate = outFormat.format(inDate);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return new Text(outDate);
    }

    public static void main(String[] args) {
        DateUdf udf = new DateUdf();
        Text evaluate = udf.evaluate(new Text("31/Aug/2015:00:04:37 +0800"));
        System.out.println(evaluate.toString());
    }
}
