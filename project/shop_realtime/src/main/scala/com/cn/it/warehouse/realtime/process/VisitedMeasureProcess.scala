package com.cn.it.warehouse.realtime.process

import com.cn.it.warehouse.realtime.bean.ClickLogEntity
import com.cn.it.warehouse.realtime.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.api.scala._
import redis.clients.jedis.Jedis

object VisitedMeasureProcess {

      def process(click_log_DataStream: DataStream[String]): DataStreamSink[ClickLogEntity] ={

          //1.把JSON流转ClickLogEntity流
        val clickLogEntityDS: DataStream[ClickLogEntity] = click_log_DataStream.map(x=>ClickLogEntity(x))

        clickLogEntityDS.print()

        clickLogEntityDS.addSink(new RichSinkFunction[ClickLogEntity] {

          var jedis :Jedis = _

          override def open(parameters: Configuration) = {
              jedis = RedisUtil.getJedis()
          }

          override def close() ={
             if(jedis.isConnected){
               jedis.close()
             }
          }

          //实现PV,UV,IP实时指标
          override def invoke(clickLogEntity: ClickLogEntity, context: SinkFunction.Context[_]) = {

            //TODO 1.PV 计算   每天0-24小时点击总量 count
            //  PV:日期
            val pvKey = "shop:pv"
            val dateTime = clickLogEntity.trackTime
            val pvHashKey = dateTime.substring(0,10).replace("-","")
            jedis.hincrBy(pvKey,pvHashKey,1)

            //TODO 2.UV计算   每天0-24小时独立访问用户量  count(distinct)
            // 怎么去重
            // 将所有的用户的guid 存放在reis中的Set中，每一条数据过来，判断是否存在，
            // 如果存在，不计数，如果不存在，计数+1，并将新的guid 存放到set中

            val uvKey = "shop:uv"
            val uvHashKey = pvHashKey
            //定义SET
            val guidSetKey = s"shop:guid:${uvHashKey}"
            val guid = clickLogEntity.guid

            if(!jedis.sismember(guidSetKey,guid)){
                //不存在，UV值累加
              jedis.hincrBy(uvKey,uvHashKey,1)
              //将用户的guid存放到set中
              jedis.sadd(guidSetKey,guid)
            }
            //TODO 3.IP计算  每天0-24小时独立IP  count(distinct)
            val ipKey = "shop:ip"
            val ipHashKey = pvHashKey
            //定义SET
            val ipSetKey = s"shop:ip:${ipHashKey}"
            val ip = clickLogEntity.ip
            if(!jedis.sismember(ipSetKey,ip)){
              //不存在，IP值累加
              jedis.hincrBy(ipKey,ipHashKey,1)
              //将用户的guid存放到set中
              jedis.sadd(ipSetKey,ip)
            }
          }
        })

      }

}
