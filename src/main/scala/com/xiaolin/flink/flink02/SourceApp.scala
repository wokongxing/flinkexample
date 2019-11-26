package com.xiaolin.flink.flink02

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
object SourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

//    env.fromCollection(List(
//      Access(201912120010L,"ruozedata.com",2000),
//      Access(201912120011L,"ruozedata.com",3000)
//    )
//    ).print()

//    env.fromElements(1,2L,3D,4F,"5").print()

    //env.addSource(new AccessSource02).setParallelism(3).print()

//    env.addSource(new ScalikeJDBCMySQLSource).print()

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ruozedata001:9092,ruozedata001:9093,ruozedata001:9094")
    properties.setProperty("group.id", "ruozedata-flink-test")
    val consumer = new FlinkKafkaConsumer010[String]("ruozedata_offset", new SimpleStringSchema(), properties)
    consumer.setStartFromEarliest()

    env.addSource(consumer).print()

    env.execute(this.getClass.getSimpleName)
  }
}
