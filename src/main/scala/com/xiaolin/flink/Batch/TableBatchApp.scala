package com.xiaolin.flink.Batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-08-11 11:14
 **/
object TableBatchApp {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)
    // register Orders table in table environment

    // specify table program
    val orders = tEnv.from("Orders") // schema (a, b, c, rowtime)
    val result = orders
      .groupBy($"a")
      .select($"a", $"b".count as "cnt")
      .toDataSet[Row] // conversion to DataSet
      .print()
  }

}
