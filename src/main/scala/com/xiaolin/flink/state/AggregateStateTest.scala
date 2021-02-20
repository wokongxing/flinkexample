package com.xiaolin.flink.state

import org.apache.flink.api.common.functions.{AggregateFunction, RichMapFunction}
import org.apache.flink.api.common.state.{AggregatingState, AggregatingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * 统计每辆车的速度综合  车牌号  speed
  * reduceingState
  */
object AggregateStateTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("hadoop001",9999)

    stream.map(data => {
      val arr = data.split(" ")
      (arr(0),arr(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String,Long),(String,Long)] {
          //定义AggregatingState状态
        private var speedCount:AggregatingState[Long,Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new AggregatingStateDescriptor[Long, Long, Long]("agg", new AggregateFunction[Long, Long, Long] {
            //初始化一个累加器
            override def createAccumulator(): Long = 0

            //每来一条数据会调用一次
            override def add(value: Long, accumulator: Long): Long = accumulator + value

            override def getResult(accumulator: Long): Long = accumulator

            override def merge(acc1: Long, acc2: Long): Long = acc1 + acc2
          },createTypeInformation[Long])
          speedCount = getRuntimeContext.getAggregatingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          speedCount.add(value._2)
          (value._1,speedCount.get())
        }
      }).print()
    env.execute()
  }
}
