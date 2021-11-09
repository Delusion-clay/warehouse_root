package com.cn.it.warehouse.realtime.utils;


import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.net.SocketTimeoutException;

public class JedisUtil {

       private static final Logger LOG = Logger.getLogger(JedisUtil.class);

       public static Jedis getJedis(int db){
             Jedis jedis = JedisUtil.getJedis();
             if(jedis!=null){
                    jedis.select(db);
             }
             return jedis;
       }


       public static void close(Jedis jedis){
              if(jedis!=null){
                     jedis.close();
              }
       }

       /**
        * 并发很高的时候 会出现获取连接失败的情况  导致程序挂掉
        * @return
        */
       public static Jedis getJedis(){
              int timeoutCount = 0;
              while (true) {// 如果是网络超时则多试几次
                     try
                     {
                            Jedis jedis = new Jedis("hadoop-101",
                                    Integer.valueOf("6379"));
                            return jedis;
                     } catch (Exception e)
                     {
                            if (e instanceof JedisConnectionException || e instanceof SocketTimeoutException)
                            {
                                   timeoutCount++;
                                   LOG.warn("获取jedis连接超时次数:" +timeoutCount);
                                   if (timeoutCount > 10)
                                   {
                                          LOG.error("获取jedis连接超时次数a:" +timeoutCount);
                                          LOG.error(null,e);
                                          break;
                                   }
                            }else
                            {
                                   LOG.error("getJedis error", e);
                                   break;
                            }
                     }
              }
              return null;
       }


       public static void main(String[] args) {
              //HASH
              Jedis jedis = JedisUtil.getJedis();

              System.out.println(jedis.exists("shop:dim_goods:100186"));;

              JedisUtil.close(jedis);
       }




}
