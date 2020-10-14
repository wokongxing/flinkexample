package com.xiaolin.flink.checkpoint

import java.util

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.restartstrategy.RestartStrategies.RestartStrategyConfiguration
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.runtime.state.filesystem.FsStateBackend

object CheckPointApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置失败重启次数
    //重启最大次数3 次,每次间隔10s
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,10000))
    //5分钟内重启最大次数10次,每次间隔5s
    env.setRestartStrategy(RestartStrategies.failureRateRestart(10,Time.minutes(5L),Time.seconds(5)))


    // 每 1000ms 开始一次 checkpoint// 每 1000ms 开始一次 checkpoint
    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\flinkexample\\checkpoint\\test"))
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

    val input = env.socketTextStream("47.114.92.31",9999)
    input.flatMap(x=>{
      val s = x.split(",")
      if(x.contains("error")){
        throw new RuntimeException("no message")
      }
      s
    }).map(x=>(x,1)).keyBy(_._1).sum(1).print()

    env.execute(this.getClass.getSimpleName)
  }

}