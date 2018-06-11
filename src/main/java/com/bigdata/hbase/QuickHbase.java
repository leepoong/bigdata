package com.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class QuickHbase {
    public static void main(String[] args) throws Exception{
        Configuration conf = HBaseConfiguration.create();
        HTable table = new HTable(conf, "user");
//      getData(table);
//      putData(table);
//        deleteData(table);
        scanData(table);
    }

    /*
    * get 'user' , 'r1' , 'f1:name'
    * */
    public static void getData(HTable hTable) throws Exception{
        Get get = new Get(Bytes.toBytes("r1"));
        get.addFamily(Bytes.toBytes("f1"));
        Result rs = hTable.get(get);
        for (Cell cell : rs.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(cell.getTimestamp());
        }
    }

    /*
    * put 'user' , 'r1' , 'f1:name' , 'marry'
    * */
    public static void putData(HTable hTable) throws Exception{
        Put put = new Put(Bytes.toBytes("r4"));
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("name"), Bytes.toBytes("sanshang"));
        hTable.put(put);
    }

    public static void deleteData(HTable hTable) {
        Delete delete = new Delete(Bytes.toBytes("r1"));
        delete.deleteColumns(Bytes.toBytes("f1"), Bytes.toBytes("name"));
        try {
            hTable.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void scanData(HTable t) throws IOException {
        Scan scan = new Scan();
        ResultScanner resultScanner = t.getScanner(scan);
        for (Result rs : resultScanner) {
            for (Cell cell : rs.rawCells()) {
                System.out.println(CellUtil.cloneValue(cell));
            }
        }
    }
}
