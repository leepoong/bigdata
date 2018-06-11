package com.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HdfsTest {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream open = fs.open(new Path("/test.txt"));
        FSDataOutputStream append = fs.append(new Path("/test.txt"));
        BufferedReader bfr = new BufferedReader(new InputStreamReader(open));
        String line = null;
        while (null != (line = bfr.readLine())) {
            System.out.println(line);
            append.write(line.getBytes());
        }
    }
}
