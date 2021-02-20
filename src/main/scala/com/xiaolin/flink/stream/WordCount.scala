package com.xiaolin.flink.stream

import org.apache.flink.api.common.functions.{MapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
object WordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.socketTextStream("hadoop001",9999).flatMap(_.split(" "))
        .map((_,1)).keyBy(0).map(new RichMapFunction[(String,Int),Int] {

      private var valuestate : ValueState[Int] =_

      override def open(parameters: Configuration): Unit = {

        val firstValue = new ValueStateDescriptor[Int]("firstValue", createTypeInformation[Int])
        valuestate = getRuntimeContext.getState(firstValue)

      }
      override def map(value: (String, Int)): Int = {
        valuestate.update(value._2+valuestate.value())

        valuestate.value()
      }
    }).print()


    env.execute(this.getClass.getSimpleName)
  }




}
