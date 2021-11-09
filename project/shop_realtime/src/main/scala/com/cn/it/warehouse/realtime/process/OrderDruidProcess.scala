package com.cn.it.warehouse.realtime.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.cn.it.warehouse.realtime.bean.{CanalEntity, OrderDBEntity}
import com.cn.it.warehouse.realtime.utils.MaptranUtil
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala._


object OrderDruidProcess {

  def process(canalEntityDataStream: DataStream[CanalEntity]): DataStream[String] = {

    //只处理订单数据
    val orderCanalEntityDataStream: DataStream[CanalEntity] = canalEntityDataStream.filter(canalEntity => {
      canalEntity.table == "shop.orders"
    })

    orderCanalEntityDataStream.print("1111111111")

    val orderDBEntityDataStream: DataStream[OrderDBEntity] = orderCanalEntityDataStream.map(canalEntity => {
      //javaMAP转scala MaP
      OrderDBEntity(MaptranUtil.toScalaImmutableMap(canalEntity.colMap))
    })

    val orderJSONDataStream = orderDBEntityDataStream.map(orderEntity=>{
        JSON.toJSONString(orderEntity,SerializerFeature.DisableCircularReferenceDetect)
    })
    orderJSONDataStream
  }

}
