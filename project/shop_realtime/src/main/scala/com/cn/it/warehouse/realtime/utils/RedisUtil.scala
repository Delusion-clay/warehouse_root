package com.cn.it.warehouse.realtime.utils

import redis.clients.jedis.Jedis

object RedisUtil {


   def getJedis(): Jedis ={
     val jedis: Jedis = new Jedis("hadoop-101",6379)
     jedis
   }


  def main(args: Array[String]): Unit = {
    val jedis: Jedis = RedisUtil.getJedis()
    jedis.hset("test","111","2222")
    jedis.close()
  }
}
