package com.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/*
*   1 分布式缓存的简单应用
*   2 MultipleInputs 的使用
* */

public class WordCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
//      arg1 & arg2 是普通的文件输入路径 ， arg3 是分布式缓存文件的输入路径 ， arg4 是reduce输出路径
        String arg1 = args[0];
        String arg2 = args[1];
        String arg3 = args[2];
        String arg4 = args[3];
        Path inPath1 = new Path(arg1);
        Path inPath2 = new Path(arg2);
        Path outPath = new Path(arg4);
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        Job job = Job.getInstance(conf,WordCount.class.getSimpleName());
        job.setJarByClass(WordCount.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(1);
//        #后面跟的是hdfs链接本地上的文件名
        job.addCacheFile(new URI(arg3+"#theFile"));
//        job.setCombinerClass(MyReducer.class);
//        job.setPartitionerClass(null);
//        job.setGroupingComparatorClass(null);
//        job.setSortComparatorClass(null);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        MultipleInputs.addInputPath(job, inPath1, TextInputFormat.class, MyMapper1.class);
        MultipleInputs.addInputPath(job, inPath2, TextInputFormat.class, MyMapper2.class);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            System.exit(ToolRunner.run(new WordCount(), args));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        Map<String, String> map = new HashMap<>();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
//                这里获取到的是在hdfs上的文件路径，如何获取到缓存在本地的文件？？？
                URI cacheFile = cacheFiles[0];
//                这里获取到的cacheFile路径实则还是hdfs上被指定为分布式缓存的文件路径，是hdfs文件路径
                System.out.println(cacheFile.getPath());
//                System.out.println(FileUtils.readLines(new File("./theFile")));
//                ./theFile是链接的本地上的文件路径名
                BufferedReader br = new BufferedReader(new FileReader(new File("./theFile")));
                String line = null;
                while (null != (line = br.readLine())) {
                    String[] split = line.split(",");
                    if (!map.containsKey(split[0])) {
                        map.put(split[0], split[1]);
                    }
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println("3");
            String[] split = value.toString().split(",");
            for (String s : split) {
                if (map.containsKey(s)) {
                    context.write(new Text(s), new IntWritable(1));
                }
            }
        }
    }

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            for (String s : split) {
                context.write(new Text(s), new IntWritable(1));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
