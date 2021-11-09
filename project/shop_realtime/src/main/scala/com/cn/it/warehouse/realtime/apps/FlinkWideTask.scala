package com.cn.it.warehouse.realtime.apps

import java.util.Properties

import com.cn.it.warehouse.realtime.bean.CanalEntity
import com.cn.it.warehouse.realtime.process.{OrderDetailWideBizProcess, OrderDruidProcess}
import com.cn.it.warehouse.realtime.utils.GlobalConfigUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
object FlinkWideTask {

  def main(args: Array[String]): Unit = {
    //1.获取上下文环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //使用事件事件
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //周期性生成水印
    env.getConfig.setAutoWatermarkInterval(1000L)

    val properties: Properties = new Properties
    properties.setProperty("bootstrap.servers",GlobalConfigUtil.`bootstrap.servers`)
    properties.setProperty("zookeeper.connect",GlobalConfigUtil.`zookeeper.connect`)
    properties.setProperty("group.id","FlinkWideTask")
    properties.setProperty("enable.auto.commit",GlobalConfigUtil.`enable.auto.commit`)
    properties.setProperty("auto.commit.interval.ms",GlobalConfigUtil.`auto.commit.interval.ms`)
    properties.setProperty("auto.offset.reset",GlobalConfigUtil.`auto.offset.reset`)
    properties.setProperty("key.serializer",GlobalConfigUtil.`key.serializer`)
    properties.setProperty("key.deserializer",GlobalConfigUtil.`key.deserializer`)

    val flinkKafkaCanalConsumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String](
      GlobalConfigUtil.`input.topic.canal`,new SimpleStringSchema(),properties)
    val canalJsonDataStream: DataStream[String] = env.addSource(flinkKafkaCanalConsumer).uid("canal_chk")

    //使用水印，提取事件事件
    val canalEntityDataStream: DataStream[CanalEntity] = canalJsonDataStream
      //转实体对象
      .map(json=> CanalEntity(json))
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[CanalEntity] {
        //初始化最大时间 ，当前最大时间
        var currentTimestamp = 0L
        //定义最大迟到事件
        val maxLatestTimestamp = 10 * 1000 //最大迟到时间

        // 构建水印
        override def getCurrentWatermark = {
          // 使用最大时间 - 迟到时间  最为水印
          new Watermark(currentTimestamp - maxLatestTimestamp)
        }
        //抽取事件事件，从数据中抽取
        override def extractTimestamp(canalEntity: CanalEntity, lastItemTimestamp: Long) = {
          currentTimestamp = canalEntity.exe_time
          currentTimestamp = Math.max(currentTimestamp, lastItemTimestamp) //将当前时间与前一元素 时间比较，取最大的时间最为
          currentTimestamp
        }
      })
    //数据拉宽
    OrderDetailWideBizProcess.process(canalEntityDataStream)
    // druid数据
    val orderJSONDataDtream: DataStream[String] = OrderDruidProcess.process(canalEntityDataStream)


    val kafkaProducer = new FlinkKafkaProducer[String](
      GlobalConfigUtil.`output.topic`,
      new SimpleStringSchema(),
      properties
    )
    orderJSONDataDtream.addSink(kafkaProducer)

    env.execute("FlinkWideTask")
  }
}
