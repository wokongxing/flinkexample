package com.xiaolin.flink.hive

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.SqlDialect
import org.apache.flink.table.catalog.hive.HiveCatalog
/**
 * @program: flinkexample
 * @description:
 * @author: linzy
 * @create: 2020-12-11 16:24
 **/
object Readtdata2hive {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnvSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()

    val tableEnv = StreamTableEnvironment.create(env,tableEnvSettings)

    val name = "myhive"
    val defaultDatabase = "ods"
    val hiveConfDir = "/data/app/flink/conf/"
//   val hiveConfDir = "D:/IdeaProjects/flinkexample/target/classes"
//    val hiveConfDir =  this.getClass.getClassLoader.getResource("").getPath


    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tableEnv.registerCatalog("myhive", hive)


    // set the HiveCatalog as the current catalog of the session
    tableEnv.useCatalog("myhive")

    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)

    tableEnv.executeSql("show databases").print()
//    tableEnv.executeSql("create table ods.labourer (name String, age int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ")
    tableEnv.executeSql("show tables").print()
    tableEnv.executeSql("select * from ods.labourer").print()




  }

}
