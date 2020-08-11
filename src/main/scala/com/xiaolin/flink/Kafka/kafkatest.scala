package com.xiaolin.flink.Kafka

import java.util
import java.util.Properties

import com.xiaolin.flink.utils.KafkaUtil
import org.apache.kafka.clients.consumer.internals.PartitionAssignor.Subscription
import org.apache.kafka.clients.consumer.{KafkaConsumer, RangeAssignor, RoundRobinAssignor}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

object kafkatest {

  var partitionsPerTopic = new util.HashMap[String, Integer]()
  var subscriptions = new util.HashMap[String, Subscription]

  def main(args: Array[String]): Unit = {
   //RangeAssignor(2,3,1)
    //RoundRobinAssignor(1,3,1)
    RoundRobinAssignor(2,"1,2,3",2)
  }
  //

  //轮询策略 //全部订阅
  def RoundRobinAssignor(topics:Int,partitions:Int,consumers:Int): Unit ={
      val rr = new RoundRobinAssignor()
    endata(topics,partitions,consumers)
    println(rr.assign(partitionsPerTopic,subscriptions))
  }
  //轮询 自定义分区 局部订阅 c0--t0; c1--t0,t1; c2 --t0,t1,t2
  def RoundRobinAssignor(topics:Int, partitions:String, consumers:Int): Unit ={
    val rr = new RoundRobinAssignor()
    //映射订阅
//    for (c <- 0 to consumers) {
//      val cstr = "C" + c
//      val list = new util.ArrayList[String]
//      for (t <- 0 to topics) {
//        val topic = "T" + t
//        list.add(topic)
//      }
//      val Subscription = new Subscription(list, null)
//      subscriptions.put(cstr, Subscription)
//    }
    import scala.collection.JavaConverters._
    subscriptions.put("C0", new Subscription(List("T0").asJava, null))
    subscriptions.put("C1", new Subscription(List("T0","T1").asJava, null))
    subscriptions.put("C2", new Subscription(List("T0","T1","T2").asJava, null))
    //映射 topic -->partition
    for (t <- 0 to topics) {
      val list = new util.ArrayList[String]
      val topic = "T" + t
      list.add(topic)
      partitionsPerTopic.put(topic, partitions.split(",")(t).toInt)
    }

    println(rr.assign(partitionsPerTopic,subscriptions))
  }

  //range策略
  def RangeAssignor(topics:Int,partitions:Int,consumers:Int): Unit ={
      endata(topics,partitions,consumers)
      val ra = new RangeAssignor()
    println(ra.assign(partitionsPerTopic,subscriptions))

  }


  def producer: Unit ={
    val kafkaProducerConfig = {
      val p = new Properties()
      p.setProperty("bootstrap.servers", "hadoop001:9092,hadoop001:9093")
      p.setProperty("key.serializer", classOf[StringSerializer].getName)
      p.setProperty("value.serializer", classOf[StringSerializer].getName)
      p
    }

    val productor = KafkaUtil[String, String](kafkaProducerConfig)
    val domains = Array("xiaolin.com","xiaoxiao.com","xiaozi.com")
    var ram = new Random()
    for (a <- 1 to 10){
      val message = new StringBuffer
      message.append("id"+a).append("\t")
        .append(domains(ram.nextInt(3)))
      productor.send("ssckafka",message.toString)
    }
  }

  def endata(topics:Int,partitions:Int,consumers:Int) {
    for (c <- 0 to consumers) {
      val cstr = "C" + c
      val list = new util.ArrayList[String]
      for (t <- 0 to topics) {
        val topic = "T" + t
        list.add(topic)
        partitionsPerTopic.put(topic, partitions)
      }
      val Subscription = new Subscription(list, null)
      subscriptions.put(cstr, Subscription)
    }
  }

  def consumers(): Unit ={

    val props = new Properties()
    props.put("bootstrap.servers", "broker1:9092,broker2:9092")
    props.put("group.id", "CountryCounter")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)

  }
}
