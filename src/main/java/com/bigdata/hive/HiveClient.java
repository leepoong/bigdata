package com.bigdata.hive;

import java.sql.*;

/*
* java 代码连接hive数据仓库
*
* */

public class HiveClient {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    public static void main(String[] args) throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Connection conn = DriverManager.getConnection("jdbc:hive2://leepong1:10000/school", "beifeng", "");
        PreparedStatement ps = conn.prepareStatement("select * from student");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
            System.out.println(rs.getString(2));
            System.out.println(rs.getInt(3));
            System.out.println(rs.getString("student.gender"));
        }
        conn.close();
    }
}
