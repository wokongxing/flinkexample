package com.xiaolin.flink.partitioner

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.mapred.OutputFormat

/**
 * @program: flinkexample
 * @description: flink 分区策略 8种
 * @author: linzy
 * @create: 2020-12-11 11:33
 **/
object PartitionerApp {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
   val result =  env.generateSequence(1,10).setParallelism(2)

    result.writeUsingOutputFormat(new TextOutputFormat[Long](new Path("./outdata/test"))).setParallelism(2)
    //随机
    result.shuffle.writeUsingOutputFormat(new TextOutputFormat[Long](new Path("./outdata/test2"))).setParallelism(4)
    result.rebalance  //随机+轮询
    result.rescale  //一般来说同一个task 分发到下游到同一个task里的
    result.global   //分发数据到下游的第一个实例分区 即下游分区仅为1
    result.forward  //分发数据到下游的分区 一一对应 不对应报错
    result.broadcast //分发每一个数据到下游的每一个分区
    result.keyBy(0)
    result.partitionCustom(new selfpartitioner(),1)

    env.execute()
  }

}

//自定义分区器
class selfpartitioner extends Partitioner[String]{

  override def partition(k: String, i: Int): Int = {
    1
  }
}
