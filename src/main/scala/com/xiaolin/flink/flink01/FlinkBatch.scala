package com.xiaolin.flink.flink01

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._
object FlinkBatch {

  def main(args: Array[String]): Unit = {
      val env = ExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(1)
    //env.readTextFile("data/test.txt")

      env.fromElements(
        "Who's there?",
        "I think I hear them. Stand, ho! Who's there?")
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1).print()


//    env.execute(this.getClass.getSimpleName)
  }

}
