package com.bigdata.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
/*
* 找出共同朋友的例子
A:B,C,D,F,E,O

B:A,C,E,K

C:F,A,D,I
...
* */
public class UserCount extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Job job = Job.getInstance(conf,"userCount");
        job.setJarByClass(UserCount.class);

        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(1);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outPath);

        boolean flag = job.waitForCompletion(true);
        return flag ? 0 : 1;
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();
        try {
            int run = ToolRunner.run(conf, new UserCount(), args);
            System.exit(run);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String string = value.toString();
            if (StringUtils.isBlank(string)) {
                return;
            }
            String[] split = string.split(":");
            String user = split[0];
            String[] friends = split[1].split(",");
            for (String friend : friends) {
                if (user.compareTo(friend) < 0) {
                    context.write(new Text(user + friend), new Text(split[1]));
                    System.out.println(user + friend);
                    System.out.println(split[1]);
                } else {
                    context.write(new Text(friend + user), new Text(split[1]));
                    System.out.println(friend+user);
                    System.out.println(split[1]);
                };
            }
            System.out.println("=========================================");
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString());
            }
            String[] split = sb.toString().split("");
            Set<String> set = new HashSet<>();
            StringBuilder sb2 = new StringBuilder();
            for (String s : split) {
                boolean rs = set.add(s);
                if (!rs) {
                    sb2.append(s);
                }
            }
            System.out.println(key.toString() + "===" + sb2.toString());
            context.write(key, new Text(sb2.toString()));
        }
    }

}
