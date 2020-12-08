package com.xiaolin.flink.stream

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object WordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.socketTextStream("hadoop001",8888).flatMap(_.split(" "))
        .map((_,1)).setParallelism(2).keyBy(0).sum(1).setParallelism(2).print()


    env.execute(this.getClass.getSimpleName)
  }




}
