package com.xiaolin.flink.flink03

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

import scala.collection.mutable

object FlinkSourceapi {
  def main(args: Array[String]): Unit = {
    val stream = StreamExecutionEnvironment.getExecutionEnvironment
    stream.setParallelism(2)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop001:9092,hadoop001:9093")
    properties.setProperty("group.id", "ruozedata-flink-test")
//    val consumer = new FlinkKafkaConsumer010[String]("ssckafka", new SimpleStringSchema(), properties)
//    val logStream = stream.addSource(consumer).map(x => {
//      val splits = x.split("\t")
//      val domain = splits(1).trim
//      (domain, x)
//    }).map(x=>(x._1,x._2))
//
//    //mysql
    import org.apache.flink.api.common.state.MapStateDescriptor
    val broadcastStateDesc = new MapStateDescriptor[String, String]("broadcast-state-desc", classOf[String], // 广播数据的key类型
      classOf[String] // 广播数据的value类型
   )
   val value = stream.addSource(new MysqlSourceApp).broadcast

//
//    logStream.connect(value).flatMap(new CoFlatMapFunction[(String,String),mutable.HashMap[String,String],String]{
//      var userDomainMap = mutable.HashMap[String, String]()
//      override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]): Unit = {
//        userDomainMap = value
//      }
//
//      override def flatMap1(value: (String, String), out: Collector[String]): Unit = {
//        val domain = value._1
//        val userId = userDomainMap.getOrElse(domain, "-99")
//        out.collect(domain + " ----> " + userId)
//      }
//
//
//    }).print()







    stream.execute("FlinkSourceapi")
  }
}
