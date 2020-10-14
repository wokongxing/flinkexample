package com.xiaolin.flink.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object FlinkStream {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.fromElements("ni hao wde  w a s d f ga as ds ").keyBy(0)


    env.execute(this.getClass.getSimpleName)
  }




}
