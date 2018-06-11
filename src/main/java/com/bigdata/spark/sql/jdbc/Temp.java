package com.bigdata.spark.sql.jdbc;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class Temp {
    public static void main(String[] args) throws Exception{
        List<String> date = Arrays.asList("2018-05-01", "2018-05-02");
        List<String> name = Arrays.asList("jack","marry","leo","tom","white","leepong","zhangsan","lisi","wangernma","sanshang","weijiu");
        List<String> word = Arrays.asList("barbecue","lemon","seafood","cake","av","girl","boy","cock","sock");
        List<String> city = Arrays.asList("beijing","tianjin","tokyo","shanghai");
        List<String> os = Arrays.asList("android","iphone");
        List<String> version = Arrays.asList("1.0", "2.0", "3.0");
        List<String> result = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 500; i++) {
            String d = date.get(random.nextInt(date.size()));
            String n = name.get(random.nextInt(name.size()));
            String w = word.get(random.nextInt(word.size()));
            String c = city.get(random.nextInt(city.size()));
            String o = os.get(random.nextInt(os.size()));
            String v = version.get(random.nextInt(version.size()));
            String str = d + "\t" + n + "\t" + w + "\t" + c + "\t" + o + "\t" + v + "\r\n";
            result.add(str);
        }
        ByteBuffer bb = ByteBuffer.allocate(1024);
        FileChannel channel = new FileOutputStream("girls.txt").getChannel();
        for (int i = 0; i < result.size(); i++) {
            bb.put(result.get(i).getBytes());
            bb.flip();
            channel.write(bb);
            bb.clear();
        }
        channel.close();
    }
}
