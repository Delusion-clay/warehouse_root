package com.cn.it.warehouse.phoenix_sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @description:
 * @author: Delusion
 * @date: 2021-08-05 15:35
 */
public class PhoneixJdbcTest {
    public static void main(String[] args) throws Exception {
        // 1. 加载Phoenix驱动类
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //Class.forName("org.apache.phoenix.jdbc.PhoenixEmbeddedDriver");
        String url = "jdbc:phoenix:hadoop-101:2181";
        // String url = "jdbc:phoenix:hadoop-11,hadoop-12,hadoop-13:2181";
        // 2. 创建数据库连接
        Connection connection = DriverManager.getConnection(url);
        // 3. 获取Statement对象
        Statement statement = connection.createStatement();
        String sql = "select * from \"anshun\" limit 10";

        // 4. 执行SQL，获取结果
        ResultSet resultSet = statement.executeQuery(sql);
        while(resultSet.next()){
            String rowkey = resultSet.getString("rowkey");
            String city = resultSet.getString("city");
            String year = resultSet.getString("year");
            System.out.println(rowkey +"\t"+city+"\t"+year);
        }
        // 6. 关闭连接
        resultSet.close();
        statement.close();
        connection.close();
    }
}
