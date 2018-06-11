package com.ibeifeng.study;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class TestDateUdf extends UDF {

	/**
	 * 日期格式转换 当前日期格式：31/Aug/2015:00:04:37 +0800 目标日期格式：2015-08-31 00:04:37
	 * 
	 * @param time
	 * @return
	 */
	// 注意建议使用public，因为要将程序打jar包进行调用的
	public Text evaluate(Text time) {

		String output = null;

		// 定义输入的日期格式，第一个参数表示传进来的日期格式是什么类型的
		SimpleDateFormat inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		// 定义输出的日期:2015-08-31 00:04:37
		SimpleDateFormat outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

		// 首先判断列中是不是有null，如果有直接返回null
		if (time == null) {
			return null;
		}

		// 判断字符串中有没有值
		if (StringUtils.isBlank(time.toString())) {
			return null;
		}

		// 去除字段中的双引号，否则无法解析，双引号需要进行转义
		String parser = time.toString().replaceAll("\"", "");

		// 将传递过来的time进行识别解析，将读入进来的信息进行保存，保存到一个变量中，返回类型是date类型
		try {
			Date parseDate = inputDate.parse(parser);
			// 使用outputDate来将date类型进行转换
			output = outputDate.format(parseDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return new Text(output);
	}

	public static void main(String[] args) {
		System.out.println(new TestDateUdf().evaluate(new Text("31/Aug/2015:00:04:37 +0800")));
	}
}
