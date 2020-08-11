package com.xiaolin.flink.source

import java.util.Properties

import com.xiaolin.flink.entity.Access
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011

/**
  *
  */
object SourceApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List(
      Access(201912120010L,"ruozedata.com",2000),
      Access(201912120011L,"ruozedata.com",3000)
    )
    ).print()

    env.fromElements(1,2L,3D,4F,"5").print()

    env.addSource(new AccessSource02).setParallelism(3).print()

    env.addSource(new ScalikeJDBCMySQLSource).print()


    env.execute(this.getClass.getSimpleName)
  }
}
