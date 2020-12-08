package com.xiaolin.flink.source

import java.util.{Optional, Properties}
import java.util.regex.Pattern

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter
import org.apache.flink.api.scala._
/**
 * KafKa消费
 *
 * @author linzy
  *
  */
object SourceKafkaApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stateBackend = new FsStateBackend("file:///out/checkpoint")
    env.setStateBackend(stateBackend)
    env.enableCheckpointing(1 * 6 * 1000, CheckpointingMode.EXACTLY_ONCE)
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop001:9092")
    properties.setProperty("group.id", "flink-00")

    // 自动发现消费的partition变化,topic新增 只能通过正则匹配
    properties.setProperty("flink.partition-discovery.interval-millis",(10 * 1000).toString)
    val topic = "ssckafka";
    val topic2 = "PREWARNIN";
    val pattern = Pattern.compile("ssckafka\\d")
//    val consumer = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)
////    最早的位置开始读取
//    consumer.setStartFromEarliest()
//    最新的位置读取
//    consumer.setStartFromLatest()

//    从时间戳大于或等于指定时间戳的位置开始读取
//    consumer.setStartFromTimestamp(1000L)
//    从 group offset 位置读取数据
//    consumer.setStartFromGroupOffsets()
//    从指定分区的 offset 位置开始读取
//    val partitionToLong[KafkaTopicPartition,Long] = HashMap(
//        new KafkaTopicPartition(topic, 0) -> 0L,
//        new KafkaTopicPartition(topic, 1) -> 1L)
//    consumer.setStartFromSpecificOffsets(partitionToLong)

//  自定义topic 动态
    val consumerpattern = new FlinkKafkaConsumer[String](pattern, new SimpleStringSchema(), properties)
//    consumerpattern.setCommitOffsetsOnCheckpoints(false) //默认值true ,为false时 offset不会自动提交

    val rateLimiter = new GuavaFlinkConnectorRateLimiter
//    consumerpattern.setRateLimiter() 限流
    env.addSource(consumerpattern).print()

    //发送消息到下游
    val producer = new FlinkKafkaProducer[String](topic2, new SimpleStringSchema(), properties)
//    env.addSource(consumer).addSink(producer)

    env.execute(this.getClass.getSimpleName)
  }
}
