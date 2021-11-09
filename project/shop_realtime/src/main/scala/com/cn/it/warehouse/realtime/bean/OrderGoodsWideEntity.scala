package com.cn.it.warehouse.realtime.bean

/**
  * 拉宽后的数据样例类
  */
case class OrderGoodsWideEntity(ogId:String,              //订单明细id
                                orderId:String,             //订单id
                                goodsId:String,             //商品id
                                goodsNum:String,          //商品数量
                                goodsPrice:String,        //商品价格
                                goodsName:String,         //商品名称
                                shopId:String,            //店铺id
                                goodsThirdCatId:String,   //商品一级分类id
                                goodsThirdCatName:String, //商品一级分类名称
                                goodsSecondCatId:String,          //商品二级分类id
                                goodsSecondCatName:String,          //商品二级分类名称
                                goodsFirstCatId:String,         //商品三级分类id
                                goodsFirstCatName:String,         //商品三级分类名称
                                areaId:String,          //店铺区域id
                                shopName:String,          //店铺名称
                                shopCompany:String,         //店铺公司名
                                cityId:String,          //店铺所属城市机构id
                                cityName:String,          //店铺所属城市机构名称
                                regionId:String,          //店铺所属大区机构id
                                regionName:String) //店铺所属大区机构名称