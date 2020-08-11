package com.xiaolin.flink.source

import com.xiaolin.flink.entity.Access
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
class AccessSource03 extends RichParallelSourceFunction[Access]{

  var running = true

  override def cancel(): Unit = {
     running = false
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

  override def run(ctx: SourceFunction.SourceContext[Access]): Unit = {
    val random = new Random()
    val domains = Array("ruozedata.com","zhibo8.cc","dongqiudi.com")

    while(running) {
      val timestamp = System.currentTimeMillis()
      1.to(10).map(x => {
        ctx.collect(Access(timestamp, domains(random.nextInt(domains.length)), random.nextInt(1000)+x))
      })

      // 休息下
      Thread.sleep(5000)
    }
  }
}
