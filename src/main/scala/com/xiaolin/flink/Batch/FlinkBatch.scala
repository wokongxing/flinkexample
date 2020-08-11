package com.xiaolin.flink.Batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.api.scala._
import org.apache.flink.table.api.bridge.scala.BatchTableEnvironment
object FlinkBatch {

  def main(args: Array[String]): Unit = {
      val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment = BatchTableEnvironment.create(env)
      env.setParallelism(1)

//    env.readTextFile("data/test.txt").map(x=>x.split(",")(0).toString).print()
      env.fromElements(
        "Who's there?",
        "I think I hear them. Stand, ho! Who's there?")

        .flatMap(_.toLowerCase.split("\\W+"))
        .filter(_.nonEmpty)
        .map((_,1))
        .groupBy(0)
        .sum(1)
//
//
        .writeAsText("outdata/wordcount.txt",WriteMode.OVERWRITE)


env.execute(this.getClass.getSimpleName)
  }

}
