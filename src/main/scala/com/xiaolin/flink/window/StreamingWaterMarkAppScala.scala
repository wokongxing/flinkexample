package com.xiaolin.flink.window

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @program: flinkexample
 * @description:
 * @author: linzy
 * @create: 2021-02-19 16:34
 **/
object StreamingWaterMarkAppScala {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = environment.socketTextStream("hadoop001", 9999)

//    stream.assignTimestampsAndWatermarks(WatermarkStrategy
//      .forBoundedOutOfOrderness[(Long, String)](Duration.ofSeconds(20))
//      .withTimestampAssigner(new SerializableTimestampAssigner[(Long, String)] {
//        override def extractTimestamp(element: (Long, String), recordTimestamp: Long): Long = element._1
//      }))
  }

}
