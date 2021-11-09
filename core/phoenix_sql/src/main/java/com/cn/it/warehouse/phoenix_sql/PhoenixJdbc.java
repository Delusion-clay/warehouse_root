package com.cn.it.warehouse.phoenix_sql;

import java.sql.*;

/**
 * 需求：
 *  查询订单明细
 */
public class PhoenixJdbc {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        // 1. 加载Phoenix驱动类
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //Class.forName("org.apache.phoenix.jdbc.PhoenixEmbeddedDriver");
        String url = "jdbc:phoenix:hadoop-101:2181";
        // String url = "jdbc:phoenix:hadoop-11,hadoop-12,hadoop-13:2181";
        // 2. 创建数据库连接
        Connection connection = DriverManager.getConnection(url);
        // 3. 获取Statement对象
        Statement statement = connection.createStatement();
        String sql = "select * from \"YW\" limit 10";

        // 4. 执行SQL，获取结果
        ResultSet resultSet = statement.executeQuery(sql);

        // 5. 遍历打印结果集
        while(resultSet.next()) {
            String ogId = resultSet.getString("ogId");
            String orderId = resultSet.getString("orderId");
            String goodsId = resultSet.getString("goodsId");
            String goodsNum = resultSet.getString("goodsNum");
            String goodsPrice = resultSet.getString("goodsPrice");
            String goodsName = resultSet.getString("goodsName");
            String shopId = resultSet.getString("shopId");
            String goodsThirdCatId = resultSet.getString("goodsThirdCatId");
            String goodsThirdCatName = resultSet.getString("goodsThirdCatName");
            String goodsSecondCatId = resultSet.getString("goodsSecondCatId");
            String goodsSecondCatName = resultSet.getString("goodsSecondCatName");
            String goodsFirstCatId = resultSet.getString("goodsFirstCatId");
            String goodsFirstCatName = resultSet.getString("goodsFirstCatName");
            String areaId = resultSet.getString("areaId");
            String shopName = resultSet.getString("shopName");
            String shopCompany = resultSet.getString("shopCompany");
            String cityId = resultSet.getString("cityId");
            String cityName = resultSet.getString("cityName");
            String regionId = resultSet.getString("regionId");
            String regionName = resultSet.getString("regionName");

            System.out.print(ogId + "\t");
            System.out.print(orderId + "\t");
            System.out.print(goodsId + "\t");
            System.out.print(goodsNum + "\t");
            System.out.print(goodsPrice + "\t");
            System.out.print(goodsName + "\t");
            System.out.print(shopId + "\t");
            System.out.print(goodsThirdCatId + "\t");
            System.out.print(goodsThirdCatName + "\t");
            System.out.print(goodsSecondCatId + "\t");
            System.out.print(goodsSecondCatName + "\t");
            System.out.print(goodsFirstCatId + "\t");
            System.out.print(goodsFirstCatName + "\t");
            System.out.print(areaId + "\t");
            System.out.print(shopName + "\t");
            System.out.print(shopCompany + "\t");
            System.out.print(cityId + "\t");
            System.out.print(cityName + "\t");
            System.out.print(regionId + "\t");
            System.out.print(regionName + "\t");
            System.out.println();
        }

        // 6. 关闭连接
        resultSet.close();
        statement.close();
        connection.close();
    }
}
