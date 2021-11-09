package com.cn.it.warehouse.realtime.process

import com.cn.it.warehouse.hbase.HbaseConf
import com.cn.it.warehouse.realtime.bean.{CanalEntity, DimGoodsCatDBEntity, DimGoodsDBEntity, DimOrgDBEntity, DimShopsDBEntity, OrderGoodsWideEntity}
import com.cn.it.warehouse.realtime.utils.RedisUtil
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._

/**
  * @description: 事实表拉宽实现
  */
object OrderDetailWideBizProcess {

  def process(canalEntityDataStream: DataStream[CanalEntity]): Unit ={
     //只处理订单详情数据
    val orderGoodesCanalEntityDataStream: DataStream[CanalEntity] = canalEntityDataStream.filter(canalEntity => {
      canalEntity.table == "shop.order_goods"
    })

    val orderGoodesWideEntityDataStream: DataStream[OrderGoodsWideEntity] = orderGoodesCanalEntityDataStream
      .timeWindowAll(Time.seconds(5))
      .apply((timeWindow, iterable, collector: Collector[OrderGoodsWideEntity]) => {
        println("33333")
        println("===========进入拉宽任务")
        val jedis: Jedis = RedisUtil.getJedis()

        val iterator: Iterator[CanalEntity] = iterable.iterator
        //遍历迭代器，进行数据遍历，拉宽
        while (iterator.hasNext) {
          val canalEntity: CanalEntity = iterator.next()
          //和REDIS中的维度表进行关联，拉宽
          //获取所有的关联字段
          val ogId = canalEntity.colMap.getOrElse("ogId", null).value
          val orderId = canalEntity.colMap.getOrElse("orderId", null).value
          val goodsId = canalEntity.colMap.getOrElse("goodsId", null).value
          val goodsNum = canalEntity.colMap.getOrElse("goodsNum", null).value
          val goodsPrice = canalEntity.colMap.getOrElse("goodsPrice", null).value

          //与redis中的维度进行比较，拉宽
          val dim_goods_key = "shop:dim_goods"
          val dim_goods_field = goodsId

          println(jedis.hexists(dim_goods_key,dim_goods_field))


          if (jedis.hexists(dim_goods_key,dim_goods_field)) {
            //获取维度表数据
            val str: String = jedis.hget("shop:dim_goods", goodsId)
            //商品实例  商品实例包含了
            val dimGoodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(str)

            //通过goodsCatId 和商品维度表的 三级分类ID 进行关联,关联到商品分类维度表
            val goodsCatJson: String = jedis.hget("shop:dim_goods_cats", dimGoodsDBEntity.goodsCatId + "")
            val thirdDimGoodsCatDBEntity: DimGoodsCatDBEntity = DimGoodsCatDBEntity(goodsCatJson)

            //三级找二级   二级维度表
            val secondGoodsCatJson: String = jedis.hget("shop:dim_goods_cats", thirdDimGoodsCatDBEntity.parentId + "")
            val secondDimGoodsCatDBEntity: DimGoodsCatDBEntity = DimGoodsCatDBEntity(secondGoodsCatJson)

            //二级找一级   一级维度表
            val firstGoodsCatJson: String = jedis.hget("shop:dim_goods_cats", secondDimGoodsCatDBEntity.parentId + "")
            val firstDimGoodsCatDBEntity: DimGoodsCatDBEntity = DimGoodsCatDBEntity(firstGoodsCatJson)

            //拉宽区域维度  关联到店铺
            //通过goodsCatId 和商品维度表的 三级分类ID 进行关联,关联到商品分类维度表
            val shopJson: String = jedis.hget("shop:dim_shops", dimGoodsDBEntity.shopId + "")
            val dimShopsDBEntity: DimShopsDBEntity = DimShopsDBEntity(shopJson)
            println("shopJson=====" + shopJson)

            //店铺找city
            val cityOrgJson: String = jedis.hget("shop:dim_org", dimShopsDBEntity.areaId + "")
            val cityDimOrgDBEntity: DimOrgDBEntity = DimOrgDBEntity(cityOrgJson)
            println("cityOrgJson=====" + cityOrgJson)

            //city找大区
            val regionOrgJson: String = jedis.hget("shop:dim_org", cityDimOrgDBEntity.parentId + "")
            println("regionOrgJson=====" + regionOrgJson)
            val regionDimOrgDBEntity: DimOrgDBEntity = DimOrgDBEntity(regionOrgJson)

            //构建大宽表
            val entity = OrderGoodsWideEntity(ogId, orderId, goodsId, goodsNum, goodsPrice, dimGoodsDBEntity.goodsName,
              dimShopsDBEntity.shopId + "", thirdDimGoodsCatDBEntity.catId + "", thirdDimGoodsCatDBEntity.catName,
              secondDimGoodsCatDBEntity.catId + "", secondDimGoodsCatDBEntity.catName,
              firstDimGoodsCatDBEntity.catId + "", firstDimGoodsCatDBEntity.catName,
              dimShopsDBEntity.areaId + "", dimShopsDBEntity.shopName, dimShopsDBEntity.shopCompany,
              cityDimOrgDBEntity.orgId + "", cityDimOrgDBEntity.orgName,
              regionDimOrgDBEntity.orgId + "", regionDimOrgDBEntity.orgName)
            collector.collect(entity)
          }
        }
        jedis.close()
      })
    //orderGoodesWideEntityDataStream.print()

      //写入hbase. 构建sink     source   transform   sink
    orderGoodesWideEntityDataStream.addSink(new RichSinkFunction[OrderGoodsWideEntity] {

      var connection: Connection = _
      override def open(parameters: Configuration) = {
         //定义 hbase连接
          connection = HbaseConf.getInstance().getHconnection
      }

      override def close() = {
        //hbase关闭
        if(connection!=null && !connection.isClosed){
          connection.close()
        }
      }

      override def invoke(orderGoodsWideEntity: OrderGoodsWideEntity, context: SinkFunction.Context[_]) = {
        //实现数据处理逻辑
        //1.获取table
        val table: Table = connection.getTable(TableName.valueOf("dwd_detail"))

        //构建put对象
        val rowkey = Bytes.toBytes(orderGoodsWideEntity.ogId.toString)
        val put = new Put(rowkey)
        val colFamilyName = Bytes.toBytes("detail")
        //往put中添加列
        put.addColumn(colFamilyName, Bytes.toBytes("ogId"), Bytes.toBytes(orderGoodsWideEntity.ogId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("orderId"), Bytes.toBytes(orderGoodsWideEntity.orderId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsId"), Bytes.toBytes(orderGoodsWideEntity.goodsId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsNum"), Bytes.toBytes(orderGoodsWideEntity.goodsNum.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsPrice"), Bytes.toBytes(orderGoodsWideEntity.goodsPrice.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsName"), Bytes.toBytes(orderGoodsWideEntity.goodsName.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("shopId"), Bytes.toBytes(orderGoodsWideEntity.shopId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsThirdCatId"), Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsThirdCatName"), Bytes.toBytes(orderGoodsWideEntity.goodsThirdCatName.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsSecondCatId"), Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsSecondCatName"), Bytes.toBytes(orderGoodsWideEntity.goodsSecondCatName.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsFirstCatId"), Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("goodsFirstCatName"), Bytes.toBytes(orderGoodsWideEntity.goodsFirstCatName.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("areaId"), Bytes.toBytes(orderGoodsWideEntity.areaId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("shopName"), Bytes.toBytes(orderGoodsWideEntity.shopName.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("shopCompany"), Bytes.toBytes(orderGoodsWideEntity.shopCompany.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("cityId"), Bytes.toBytes(orderGoodsWideEntity.cityId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("cityName"), Bytes.toBytes(orderGoodsWideEntity.cityName.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("regionId"), Bytes.toBytes(orderGoodsWideEntity.regionId.toString))
        put.addColumn(colFamilyName, Bytes.toBytes("regionName"), Bytes.toBytes(orderGoodsWideEntity.regionName.toString))
        table.put(put)
        table.close()
      }
    })

  }
}
