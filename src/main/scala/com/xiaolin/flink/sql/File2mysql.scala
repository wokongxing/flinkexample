package com.xiaolin.flink.sql

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment


/**
 * @program: flinkexample
 * @description:
 * @author: linzy
 * @create: 2020-12-10 15:18
 **/
object File2mysql {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    val sql =
      """
        | create table dic_area (
        |    id int ,
        |    code string,
        |    name string,
        |    parentcode string,
        |    level int
        | ) with (
        |   'connector' = 'filesystem',
        |   'path' = 'D:\IdeaProjects\flinkexample\data\dict_area.csv',
        |   'format' = 'csv'
        | )
        |
        |""".stripMargin

    val sql2 =
      """
        | create table dic_area_3 (
        |    id int ,
        |    code string,
        |    name string,
        |    parentcode string,
        |    level int
        | ) with (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://115.159.42.226:3306/sparkdb',
        |   'table-name' = 'dict_area_3',
        |   'username' = 'root',
        |   'password' = '123456',
        |   'driver' = 'com.mysql.jdbc.Driver',
        |   'lookup.cache.max-rows' = '500',
        |   'lookup.cache.ttl' = '3s',
        |   'lookup.max-retries' = '3',
        |   'sink.buffer-flush.max-rows'='100',
        |   'sink.buffer-flush.interval'= '1s'
        | )
        |
        |""".stripMargin

    val sql3 =
      """
        | insert into  dic_area_3
        | SELECT
        |   id,
        |   code,
        |   name,
        |   parentcode,
        |   level
        |  from dic_area
        |
        |""".stripMargin
    tableEnv.executeSql(sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)


  }

}
