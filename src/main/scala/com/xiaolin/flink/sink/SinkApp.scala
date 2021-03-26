package com.xiaolin.flink.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥
  * 交流群：545916944
  */
object SinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setMaxParallelism(1)
//    val stream = env.readTextFile("data/access.log").map(x => {
//      val splits = x.split(",")
//      Access(splits(0).toLong, splits(1), splits(2).toLong).toString
//    })
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "ruozedata001:9092,ruozedata001:9093,ruozedata001:9094")
//    properties.setProperty("group.id", "ruozedata_offset")
//    val consumer = new FlinkKafkaConsumer010[String]("ruozedata_offset", new SimpleStringSchema(), properties)
//    val stream = env.addSource(consumer)
//
//    // TODO... Kafka2Kafka  ruozedata_offset ==> ruozedata_offset_test
//
//    val producer = new FlinkKafkaProducer010[String](
//      "ruozedata001:9092,ruozedata001:9093,ruozedata001:9094",         // broker list
//      "ruozedata_offset_test",               // target topic
//      new SimpleStringSchema)   // serialization schema
//
//    stream.addSink(producer)  // 2Kafka
//    stream.print() // 2Local


    val stream = env.readTextFile("data/access.log").map(x => {
      val splits = x.split(",")
      (splits(1).trim, splits(2).toDouble)
    }).keyBy("0").sum(1)

    //stream.addSink(new RuozedataMySQLSink)

//    val conf = new FlinkJedisPoolConfig.Builder().setHost("114.55.94.172").build()
//
//    stream.addSink(new RedisSink[(String, Double)](conf, new RuozedataRedisSink))


    env.execute(this.getClass.getSimpleName)
  }
}
