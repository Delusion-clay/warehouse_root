package com.cn.it.warehouse.kylin;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
/**
 * @description:
 * @author: Delusion
 * @date: 2021-01-23 22:42
 */
public class JdbcKylin {
    public static void main(String[] args) throws Exception {
        //1.加载驱动
        Class.forName("org.apache.kylin.jdbc.Driver");
        //2.获取数据库连接
        Connection connection = DriverManager.getConnection("jdbc:kylin://project-02:7070/test1111", "ADMIN", "KYLIN");
        //3.构建SQL语句
        String sql = "select t1.date1,\n" +
                "t2.regionid,\n" +
                "t2.regionname,\n" +
                "t3.productid,\n" +
                "t3.productname,\n" +
                "sum(t1.price) as total_money,\n" +
                "sum(t1.amount) as total_amount \n" +
                "from dw_sales t1 \n" +
                "inner join dim_region t2 on t1.regionid = t2.regionid\n" +
                "inner join dim_product t3 on t1.productid = t3.productid \n" +
                "group by t1.date1,t2.regionid,t2.regionname,t3.productid,t3.productname order by t1.date1,t2.regionname,t3.productname;";
        //4 构建statement查询对象
        Statement statement = connection.createStatement();

        //5.执行查询，获取结果
        ResultSet resultSet = statement.executeQuery(sql);
        //6.遍历数据，打印结果
        while (resultSet.next()){
            String total_money = resultSet.getString("total_money");
            String total_amount = resultSet.getString("total_amount");
            System.out.println( total_money + "\t" + total_amount);
        }
        //7 关闭资源连接
        resultSet.close();
        statement.close();
        connection.close();
    }
}
