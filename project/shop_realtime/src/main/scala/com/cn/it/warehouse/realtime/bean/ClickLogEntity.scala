package com.cn.it.warehouse.realtime.bean

import com.alibaba.fastjson.{JSON, JSONObject}

case class ClickLogEntity(
                           referer: String,
                           linkId: String,
                           ip: String,
                           trackTime: String,
                           guid: String,
                           id: String,
                           sessionId: String,
                           attachedInfo: String,
                           url: String
                         )

object ClickLogEntity {

  def apply(json: String): ClickLogEntity = {
    val jsonBbj: JSONObject = JSON.parseObject(json)
    val referer: String = jsonBbj.getString("referer")
    val linkId: String = jsonBbj.getString("linkId")
    val ip: String = jsonBbj.getString("ip")
    val trackTime: String = jsonBbj.getString("trackTime")
    val guid: String = jsonBbj.getString("guid")
    val id: String = jsonBbj.getString("id")
    val sessionId: String = jsonBbj.getString("sessionId")
    val attachedInfo: String = jsonBbj.getString("attachedInfo")
    val url: String = jsonBbj.getString("url")
    ClickLogEntity(referer,linkId,ip,trackTime,guid,id,sessionId,attachedInfo,url)
  }


  def main(args: Array[String]): Unit = {
     val json = """{"referer":"http://star.codejoys.com.cn/135/1351735_3.html","linkId":"1.1.16.0.5.KlBK557-10-6z7Ct","ip":"210.26.1.61","trackTime":"2019-10-16 14:35:44","guid":"7d01d91a-1ae4-4d31-ba1b-884847c3f301","id":121508281810002972,"sessionId":"b19047d6-e3cb-40e9-834f-6126824052e2","attachedInfo":"0.0.12.2704_13852075_5.13","url":"http://order.codejoys.cn/checkoutV3/index.do?fastBuyFlag=1&returnUrl=http://item.codejoys.cn/item/33678857"}
                """
      print(ClickLogEntity(json))
  }


}
