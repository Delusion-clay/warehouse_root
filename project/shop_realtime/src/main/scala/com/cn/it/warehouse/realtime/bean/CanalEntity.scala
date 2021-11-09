package com.cn.it.warehouse.realtime.bean

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

case class CanalEntity(event_type: String,
                       exe_time: Long,
                       cur_time: Long,
                       binlog: String,
                       table: String,
                       colMap: java.util.Map[String, ColValueType]) //事件类型


case class ColValueType(var cloumn: String, //列名
                        var value: String, //字段名
                        var dataType: String) //类型


object CanalEntity {
  //将JSON转为实体对象
  def apply(json: String): CanalEntity = {
    //1. 将JSON字符串转为json对象
    val jsonObject: JSONObject = JSON.parseObject(json)


    //2. 解析5个字段
    val event_type = jsonObject.getString("event_type")
    val exe_time = jsonObject.getString("exe_time")
    val cur_time = jsonObject.getString("cur_time")
    val binlog = jsonObject.getString("binlog")
    val table = jsonObject.getString("table")
    val cols = jsonObject.getString("cols")


    //3.解析cols数据，解析为数组字符串
    val jsonArray: JSONArray = JSON.parseArray(cols)

    val map: util.HashMap[String, ColValueType] = new util.HashMap[String,ColValueType]()

    //遍历jsonArray
    for (i<- 0 until jsonArray.size()) {
      val jsonObjectTemp: JSONObject = jsonArray.getJSONObject(i)
      val col = jsonObjectTemp.getString("col")
      val dataVal = jsonObjectTemp.getString("val")
      val dataType = jsonObjectTemp.getString("type")
      map.put(col,ColValueType(col,dataVal,dataType))
    }
    CanalEntity(event_type,exe_time.toLong,cur_time.toLong,binlog,table,map)
  }
}



