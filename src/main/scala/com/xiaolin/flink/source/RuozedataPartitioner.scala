package com.xiaolin.flink.source

import org.apache.flink.api.common.functions.Partitioner

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
class RuozedataPartitioner extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    println("partitions: " + numPartitions)

    if(key == "ruozedata.com"){
      0
    } else if(key == "dongqiudi.com"){
      1
    } else {
      2
    }

  }
}
