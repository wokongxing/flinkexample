package com.xiaolin.flink.utils

import java.util.Properties
import java.util.concurrent.Future

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
        p.setProperty("bootstrap.servers", "hadoop001:9092,hadoop001:9093")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }

      val productor = KafkaUtil[String, String](kafkaProducerConfig)
      val domains = Array("xiaolin.com","xiaoxiao.com","xiaozi.com","test.com","wode.com","huawei.com")
      var ram = new Random()
      for (a <- 1 to 10){
        val message = new StringBuffer
        message.append("ssid"+a).append("\t")
          .append(domains(ram.nextInt(6)))
        productor.send("ssckafka1",message.toString)
      }

  }
}
