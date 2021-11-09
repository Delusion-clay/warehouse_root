package com.cn.it.warehouse.realtime.bean

import scala.beans.BeanProperty

case class OrderDBEntity(@BeanProperty orderId:Long, //订单id
                         @BeanProperty orderNo:String, //订单编号
                         @BeanProperty shopId:Long, //门店id
                         @BeanProperty userId:Long, //用户id
                         @BeanProperty orderStatus:Int, //订单状态-3:用户拒收-2:未付款的订单-1：用户取消0:待发货1:配送中2:用户确认收货
                         @BeanProperty goodsMoney:Double, //商品金额
                         @BeanProperty deliverType:Int, //收货方式0:送货上门1:自提
                         @BeanProperty deliverMoney:Double, //运费
                         @BeanProperty totalMoney:Double, //订单金额（包括运费）
                         @BeanProperty realTotalMoney:Double, //实际订单金额（折扣后金额）
                         @BeanProperty payType:Int, //支付方式
                         @BeanProperty isPay:Int, //是否支付0:未支付1:已支付
                         @BeanProperty areaId:Int, //区域最低一级
                         @BeanProperty areaIdPath:String, //区域idpath
                         @BeanProperty userName:String, //收件人姓名
                         @BeanProperty userAddress:String, //收件人地址
                         @BeanProperty userPhone:String, //收件人电话
                         @BeanProperty orderScore:Int, //订单所得积分
                         @BeanProperty isInvoice:Int, //是否开发票1:需要0:不需要
                         @BeanProperty invoiceClient:String, //发票抬头
                         @BeanProperty orderRemarks:String, //订单备注
                         @BeanProperty orderSrc:Int, //订单来源0:商城1:微信2:手机版3:安卓App4:苹果App
                         @BeanProperty needPay:Double, //需缴费用
                         @BeanProperty payRand:Int, //货币单位
                         @BeanProperty orderType:Int, //订单类型
                         @BeanProperty isRefund:Int, //是否退款0:否1：是
                         @BeanProperty isAppraise:Int, //是否点评0:未点评1:已点评
                         @BeanProperty cancelReason:Int, //取消原因ID
                         @BeanProperty rejectReason:Int, //用户拒绝原因ID
                         @BeanProperty rejectOtherReason:String, //用户拒绝其他原因
                         @BeanProperty isClosed:Int, //订单是否关闭
                         @BeanProperty goodsSearchKeys:String,
                         @BeanProperty orderunique:String, //订单流水号
                         @BeanProperty receiveTime:String, //收货时间
                         @BeanProperty deliveryTime:String, //发货时间
                         @BeanProperty tradeNo:String, //在线支付交易流水
                         @BeanProperty dataFlag:Int, //订单有效标志 -1：删除 1:有效
                         @BeanProperty createTime:String, //下单时间
                         @BeanProperty settlementId:Int, //是否结算，大于0的话则是结算ID
                         @BeanProperty commissionFee:Double, //订单应收佣金
                         @BeanProperty scoreMoney:Double, //积分抵扣金额
                         @BeanProperty useScore:Int, //花费积分
                         @BeanProperty orderCode:String,
                         @BeanProperty extraJson:String, //额外信息
                         @BeanProperty orderCodeTargetId:Int,
                         @BeanProperty noticeDeliver:Int, //提醒发货 0:未提醒 1:已提醒
                         @BeanProperty invoiceJson:String, //发票信息
                         @BeanProperty lockCashMoney:Double, //锁定提现金额
                         @BeanProperty payTime:String, //支付时间
                         @BeanProperty isBatch:Int, //是否拼单
                         @BeanProperty totalPayFee:Int) //总支付金额


object OrderDBEntity {
  def apply(map:Map[String, ColValueType]): OrderDBEntity = {
    OrderDBEntity(map.get("orderId").get.value.toLong,
      map.get("orderNo").get.value,
      map.get("shopId").get.value.toLong,
      map.get("userId").get.value.toLong,
      map.get("orderStatus").get.value.toInt,
      map.get("goodsMoney").get.value.toDouble,
      map.get("deliverType").get.value.toInt,
      map.get("deliverMoney").get.value.toDouble,
      map.get("totalMoney").get.value.toDouble,
      map.get("realTotalMoney").get.value.toDouble,
      map.get("payType").get.value.toInt,
      map.get("isPay").get.value.toInt,
      map.get("areaId").get.value.toInt,
      map.get("areaIdPath").get.value,
      map.get("userName").get.value,
      map.get("userAddress").get.value,
      map.get("userPhone").get.value,
      map.get("orderScore").get.value.toInt,
      map.get("isInvoice").get.value.toInt,
      map.get("invoiceClient").get.value,
      map.get("orderRemarks").get.value,
      map.get("orderSrc").get.value.toInt,
      map.get("needPay").get.value.toDouble,
      map.get("payRand").get.value.toInt,
      map.get("orderType").get.value.toInt,
      map.get("isRefund").get.value.toInt,
      map.get("isAppraise").get.value.toInt,
      map.get("cancelReason").get.value.toInt,
      map.get("rejectReason").get.value.toInt,
      map.get("rejectOtherReason").get.value,
      map.get("isClosed").get.value.toInt,
      map.get("goodsSearchKeys").get.value,
      map.get("orderunique").get.value,
      map.get("receiveTime").get.value,
      map.get("deliveryTime").get.value,
      map.get("tradeNo").get.value,
      map.get("dataFlag").get.value.toInt,
      map.get("createTime").get.value,
      map.get("settlementId").get.value.toInt,
      map.get("commissionFee").get.value.toDouble,
      map.get("scoreMoney").get.value.toDouble,
      map.get("useScore").get.value.toInt,
      map.get("orderCode").get.value,
      map.get("extraJson").get.value,
      map.get("orderCodeTargetId").get.value.toInt,
      map.get("noticeDeliver").get.value.toInt,
      map.get("invoiceJson").get.value,
      map.get("lockCashMoney").get.value.toDouble,
      map.get("payTime").get.value,
      map.get("isBatch").get.value.toInt,
      map.get("totalPayFee").get.value.toInt)
  }
}
