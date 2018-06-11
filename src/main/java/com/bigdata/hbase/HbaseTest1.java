package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;

public class HbaseTest1 {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "user");
//        getData(table);
//        putData(table);
        scanData(table);
    }

    public static void getData(HTable table) {
        Get get = new Get(Bytes.toBytes("r1"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
        try {
            Result rs= table.get(get);
            for (Cell cell : rs.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void putData(HTable table) {
        Put put = new Put(Bytes.toBytes("r3"));
        put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("sanshang"));
        try {
            table.put(put);
        } catch (InterruptedIOException e) {
            e.printStackTrace();
        } catch (RetriesExhaustedWithDetailsException e) {
            e.printStackTrace();
        }
    }

    public static void scanData(HTable table) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = table.getScanner(scan);
        for (Result r : rs) {
            for (Cell cell : r.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }
}
