package com.xiaolin.flink.hbase

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object HbaseFlinkApp {
  def main(args: Array[String]): Unit = {
        Stream


  }
  def Stream: Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
//    val hbaseStream = env.addSource(new HbaseReaderSourceFunction)
//    hbaseStream.print()
    env.readTextFile("data/ruoze.log")
        .map(x=>{
//      val split = x.split(",")
//          ()
          x
    }).addSink(new HbaseWriterSinkFunction)


    env.execute(this.getClass.getSimpleName)
  }

}
