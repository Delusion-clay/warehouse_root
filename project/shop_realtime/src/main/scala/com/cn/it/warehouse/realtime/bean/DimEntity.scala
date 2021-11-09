package com.cn.it.warehouse.realtime.bean

import com.alibaba.fastjson.JSON

import scala.beans.BeanProperty

/**
  * 所有维度数据的样例类都保存在DimEntity.scala文件中
  */

// 商品维度实体类
case class DimGoodsDBEntity(@BeanProperty goodsId:Long,		      // 商品id
                            @BeanProperty goodsName:String,		  // 商品名称
                            @BeanProperty shopId:Long,		      // 店铺id
                            @BeanProperty goodsCatId:Int) // 商品分类id

object DimGoodsDBEntity {
  // {"goodsCatId":294,"goodsId":100153,"goodsName":"法国原瓶进口红酒凯旋干红葡萄酒礼盒750ml整箱6支装","shopId":100060}
  def apply(json: String): DimGoodsDBEntity = {
    val jsonObject = JSON.parseObject(json)

    println("===========" + jsonObject)
    if(jsonObject==null){
        return null
    }

    val goodsId = jsonObject.getLong("goodsId")
    val goodsName = jsonObject.getString("goodsName")
    val shopId = jsonObject.getLong("shopId")
    val goodsCatId = jsonObject.getInteger("goodsCatId")

    DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId)
  }
}

// 商品分类维度实体类
case class DimGoodsCatDBEntity(@BeanProperty catId:Long,	      //商品分类id
                               @BeanProperty parentId:Long,	    //商品分类父id
                               @BeanProperty catName:String,	  //商品分类名称
                               @BeanProperty cat_level:Int) //商品分类级别

object DimGoodsCatDBEntity {
  def apply(json: String): DimGoodsCatDBEntity = {
    val jsonObject = JSON.parseObject(json)

    val catId = jsonObject.getLong("catId")
    val parentId = jsonObject.getLong("parentId")
    val catName = jsonObject.getString("catName")
    val cat_level = jsonObject.getInteger("cat_level")

    DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
  }
}

// 店铺维度样例类
case class DimShopsDBEntity(@BeanProperty shopId:Int,		// 店铺id
                            @BeanProperty areaId:Int,		// 店铺所属区域id
                            @BeanProperty shopName:String,	// 店铺名称
                            @BeanProperty shopCompany:String) // 公司名称

object DimShopsDBEntity {
  def apply(json: String): DimShopsDBEntity = {
    val jsonObject = JSON.parseObject(json)

    val shopId = jsonObject.getInteger("shopId")
    val areaId = jsonObject.getInteger("areaId")
    val shopName = jsonObject.getString("shopName")
    val shopCompany = jsonObject.getString("shopCompany")

    DimShopsDBEntity(shopId, areaId, shopName, shopCompany)
  }
}

// 组织结构维度样例类
case class DimOrgDBEntity(@BeanProperty orgId:Int,			  // 机构id
                          @BeanProperty parentId:Int,		  // 机构父id
                          @BeanProperty orgName:String,		// 组织机构名称
                          @BeanProperty orgLevel:Int) // 组织机构级别

object DimOrgDBEntity {
  def apply(json: String): DimOrgDBEntity = {
    val jsonObject = JSON.parseObject(json)

    val orgId = jsonObject.getInteger("orgId")
    val parentId = jsonObject.getInteger("parentId")
    val orgName = jsonObject.getString("orgName")
    val orgLevel = jsonObject.getInteger("orgLevel")

    DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
  }

  def main(args: Array[String]): Unit = {
    println(DimGoodsDBEntity("{\"goodsCatId\":294,\"goodsId\":100153,\"goodsName\":\"法国原瓶进口红酒凯旋干红葡萄酒礼盒750ml整箱6支装\",\"shopId\":100060}"))
    println(DimShopsDBEntity("{\"areaId\":100211,\"shopCompany\":\"北京商淘药业科技有限公司\",\"shopId\":100060,\"shopName\":\"同仁堂大健康\"}"))
    println(DimGoodsCatDBEntity("{\"catId\":150,\"catName\":\"延缓衰老\",\"cat_level\":3,\"parentId\":138}"))
    println(DimOrgDBEntity("{\"orgId\":100238,\"orgLevel\":2,\"orgName\":\"枣庄市分公司\",\"parentId\":100006}"))
  }
}