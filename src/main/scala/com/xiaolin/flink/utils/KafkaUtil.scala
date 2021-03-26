package com.xiaolin.flink.utils

import java.util.Properties
import java.util.concurrent.Future

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.util.Random

class KafkaUtil [K, V](createProducer: () => KafkaProducer[K, V]) {
  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, key, value))
  }

  def send(topic: String, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, value))
  }
}

object KafkaUtil {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaUtil[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    new KafkaUtil(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaUtil[K, V] = apply(config.toMap)





  /**
   * 发送消息
   * @param args
   */
  def main(args: Array[String]): Unit = {

      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "hadoop001:9092,hadoop001:9093,hadoop001:9094")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }

      val productor = KafkaUtil[String, String](kafkaProducerConfig)
      val domains = Array("11","2","3","4","5","6")
      var ram = new Random()
      for (a <- 100 to 150){
//        val message = new StringBuffer
//        message.append("ssid"+a).append("\t")
//          .append(domains(ram.nextInt(6)))
        val nObject = new JSONObject
        nObject.put("user_id",a);
        nObject.put("order_amount",domains(ram.nextInt(6)));
        nObject.put("log_ts","2020-11-26 04:10:00");
        productor.send("kafka_table",nObject.toString)
      }



  }
}
