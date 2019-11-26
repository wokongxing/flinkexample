package com.xiaolin.flink.flink03.core

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.util.Collector

import scala.collection.mutable

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
object JoinApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "ruozedata001:9092,ruozedata001:9093,ruozedata001:9094")
    properties.setProperty("group.id", "ruozedata-flink-test")
    val consumer = new FlinkKafkaConsumer010[String]("ruozedata_offset", new SimpleStringSchema(), properties)
    val logStream = env.addSource(consumer).map(x => {
      val splits = x.split("\t")
      val domain = splits(5)
      val flag = splits(2)
      (domain, flag, x)
    }).filter(_._2 == "T").map(x=>(x._1,x._2,x._3))

    val mysqlStream = env.addSource(new UserDomainMySQLSource).broadcast

    // connect 之后要做的事情 应该是根据log流的domain 匹配上mysql流的domain，然后取出userid
    logStream.connect(mysqlStream).flatMap(new CoFlatMapFunction[(String,String,String),mutable.HashMap[String, String], String] {
      var userDomainMap = mutable.HashMap[String, String]()
      override def flatMap1(value: (String, String, String), out: Collector[String]): Unit = {
        // sleep
        val domain = value._1

//        for(ele <- userDomainMap) {
//          println("~~~~~~~~~~~" + ele._1 + "-->" + ele._2)
//        }

        val userId = userDomainMap.getOrElse(domain, "-99")
        out.collect(domain + " ----> " + userId)
      }

      override def flatMap2(value: mutable.HashMap[String, String], out: Collector[String]): Unit = {
        userDomainMap = value
      }
    }).print()

    env.execute(this.getClass.getSimpleName)
  }
}
