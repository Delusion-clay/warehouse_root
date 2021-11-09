package com.cn.it.warehouse.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HbaseConf {

    private static final String HBASE_SITE="hbase-site.xml";
    private volatile static HbaseConf hbaseConf;

    //hbase 配置文件
    private Configuration configuration;

    private volatile Connection conn;


    private HbaseConf(){
        //获取连接
        getHconnection();
    }

    public static HbaseConf getInstance(){
         if(hbaseConf == null){
            synchronized (HbaseConf.class){
                if(hbaseConf == null){
                    hbaseConf = new HbaseConf();
                    System.out.println(hbaseConf);
                }
            }
         }
         return hbaseConf;
    }


    public Configuration getConfiguration(){
        if(configuration == null){
             configuration = HBaseConfiguration.create();
             configuration.addResource(HBASE_SITE);
            System.out.println(configuration);
        }

        return configuration;
    }



    public Connection getHconnection(){
        Configuration configuration = getConfiguration();
        if(conn == null){
              synchronized (HbaseConf.class){
                  if(conn == null){
                      try {
                          conn = ConnectionFactory.createConnection(configuration);
                          System.out.println(conn);
                      } catch (IOException e) {
                          e.printStackTrace();
                      }
                  }
              }
        }
        return conn;
    }


    public static void main(String[] args) throws IOException {
        Connection con = HbaseConf.getInstance().getHconnection();
        System.out.println(con.getAdmin().tableExists(TableName.valueOf("dwd_detail")));

    }

}
