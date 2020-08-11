package com.xiaolin.flink.state

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._

object OperatorStateApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.CheckpointingMode
    import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
    // 每 1000ms 开始一次 checkpoint// 每 1000ms 开始一次 checkpoint

    env.enableCheckpointing(1000)
    // 高级选项：
    // 设置模式为精确一次 (这是默认值)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确认 checkpoints 之间的时间会进行 500 ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许一个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 开启在 job 中止后仍然保留的 externalized checkpoints
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 允许在有更近 savepoint 时回退到 checkpoint
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)

    val input = env.fromElements(1L, 2L, 3L, 4L, 7L, 5L, 1L, 5L, 4L, 6L, 1L, 7L, 8L, 9L, 1L)

    input.flatMap(new OperatorStateMap()).print

    System.out.println(env.getExecutionPlan)

    env.execute(this.getClass.getSimpleName)
  }

  class OperatorStateMap extends RichFlatMapFunction[Long, Tuple2[Integer, String]] with CheckpointedFunction { //托管状态
    private var listState: ListState[Long] = _
    //原始状态
    private var listElements: util.ArrayList[Long] = new util.ArrayList

    @throws[Exception]
    override def flatMap(value: Long, collector: Collector[Tuple2[Integer, String]]): Unit = {
      if (value == 1) {
        if (listElements.size > 0) {
          val buffer = new StringBuffer
          for (ele <- listElements) {
            buffer.append(ele + " ")
          }
          val sum = listElements.size
          collector.collect(Tuple2[Integer, String](sum.toInt, buffer.toString))
          listElements.clear
        }
      } else {
        listElements.add(value)
      }
    }

    /**
     * 进行checkpoint进行快照
     *
     * @param context
     * @throws Exception
     */
    @throws[Exception]
    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      listState.clear()
      for (ele <- listElements) {
        listState.add(ele)
      }
    }

    /**
     * state的初始状态，包括从故障恢复过来
     *
     * @param context
     * @throws Exception
     */
    @throws[Exception]
    override def initializeState(context: FunctionInitializationContext): Unit = {
      val listStateDescriptor = new ListStateDescriptor("checkPointedList", TypeInformation.of(new TypeHint[Long]() {}))
      listState = context.getOperatorStateStore.getListState(listStateDescriptor)
      //如果是故障恢复
      if (context.isRestored) { //从托管状态将数据到移动到原始状态
        for (ele <- listState.get) {
          listElements.add(ele)
        }
        listState.clear()
      }
    }

    @throws[Exception]
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      listElements = new util.ArrayList[Long]
    }

  }

}