package com.xiaolin.flink.flink01

import org.apache.flink.streaming.api.scala._
object FlinkStream {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    env.execute(this.getClass.getSimpleName)
  }

}
