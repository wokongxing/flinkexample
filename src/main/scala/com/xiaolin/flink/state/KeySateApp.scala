package com.xiaolin.flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.util.Collector


/**
 * KeyedState Demo
 * 计算不同key的平均每三个之间的平均值
 */
object KeySateApp {

    def main(args: Array[String]): Unit = {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      env.setParallelism(2)
      val input = env.fromElements(
        Tuple2(1L, 3L),
        Tuple2(1L, 4L),
        Tuple2(1L, 5L),
        Tuple2(2L, 4L),
        Tuple2(2L, 4L),
        Tuple2(3L, 5L),
        Tuple2(2L, 3L),
        Tuple2(1L, 8L)
      )
      input
      input.keyBy(new KeySelector[Tuple2[Long, Long],Long] {
        override def getKey(value: (Long, Long)): Long ={
          value._1
        }

      }).flatMap(new KeyedStateAgvFlatMap).print()
      env.execute(this.getClass.getSimpleName)
    }

    class KeyedStateAgvFlatMap extends RichFlatMapFunction[Tuple2[Long, Long], Tuple2[Long, Long]] {
      private var valueState :ValueState[Tuple2[Long,Long]]=_

      @throws[Exception]
      override def flatMap(value: Tuple2[Long, Long], collector: Collector[Tuple2[Long, Long]]): Unit = {
        var currentValue = valueState.value
        if (currentValue == null){
          currentValue = Tuple2(0L, 0L)
        }else{
          currentValue = Tuple2(currentValue._1+1, value._2+currentValue._2)
        }
        valueState.update(currentValue)

        //大于三个
        if (currentValue._1 >= 3) {
          collector.collect(Tuple2(value._1, currentValue._2 / currentValue._1))
          valueState.clear()
        }
      }

      @throws[Exception]
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        //keyedState可以设置TTL过期时间
        val config = StateTtlConfig
          .newBuilder(Time.seconds(30))
          .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
          .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
          .build
        val valueStateDescriptor = new ValueStateDescriptor("agvKeyedState", TypeInformation.of(new TypeHint[Tuple2[Long, Long]]() {}))
        //设置支持TTL配置
        valueStateDescriptor.enableTimeToLive(config)
        valueState = getRuntimeContext.getState(valueStateDescriptor)
      }
    }
}
