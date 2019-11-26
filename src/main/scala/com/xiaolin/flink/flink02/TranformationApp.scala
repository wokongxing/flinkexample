package com.xiaolin.flink.flink02

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
object TranformationApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment


//    val stream = env.readTextFile("data/access.log").map(x => {
//      val splits = x.split(",")
//      Access(splits(0).toLong, splits(1), splits(2).toLong)
//    })

    //stream.keyBy("domain").sum("traffic").print("sum")

//    stream.keyBy("domain").reduce((x,y) => {
//      Access(x.time, x.domain, (x.traffic+y.traffic+100))
//    }).print()

    // 5000 6000 7000
//    val splitStream = stream.keyBy("domain").sum("traffic").split(x => {
//      if (x.traffic > 6000) {
//        Seq("大客户")
//      } else {
//        Seq("一般客户")
//      }
//    })
//
//    splitStream.select("大客户").print("大客户")
//    splitStream.select("一般客户").print("一般客户")
//    splitStream.select("大客户","一般客户").print("ALL")

//    val stream1 = env.addSource(new AccessSource)
//    val stream2 = env.addSource(new AccessSource)
//
//    // stream1和stream2的数据类型是一样的
//    stream1.union(stream2).map(x=>{
//      println("接收到的数据:" + x)
//      x
//    }).print()
//
//    val stream2New = stream2.map(x => ("J哥", x))
//    //stream1.union(stream2New)
//
//    stream1.connect(stream2New).map(x=>x,y=>y).print()

    env.setParallelism(3)

    env.addSource(new AccessSource)
      .map(x=>(x.domain, x))
      .partitionCustom(new RuozedataPartitioner, 0)
      .map(x => {
        println("current thread id is: " + Thread.currentThread().getId + " , value is: " + x)
        x._2
      }).print()


    env.execute(this.getClass.getSimpleName)
  }
}
