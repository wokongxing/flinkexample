package com.xiaolin.flink.cdc

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @program: flinkexample
 * @description: cdc mysql join jdbc mysql
 * @author: linzy
 * @create: 2021-01-26 16:02
 **/
object Mysql2MysqlCDCDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tableEnv = StreamTableEnvironment.create(env);
    val configuration = tableEnv.getConfig.getConfiguration
    env.enableCheckpointing(1000)
    env.setStateBackend(new FsStateBackend("file:///D:\\IdeaProjects\\flinkexample\\checkpoint\\Mysql2MysqlCDCDemo",true))
    // 高级选项：
    // 设置模式为精确一次 (这是默认值)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(600*1000)
    //允许 checkpoint 失败2次 默认值0
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(10)
    // 同一时间只允许2个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 每一个checkpoint 相隔 1分钟时间 与上一个配置互斥
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(60*1000)
    // 开启在 job 中止后仍然保留的 externalized checkpoints
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 允许在有更近 savepoint 时回退到 checkpoint
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true)

    // set low-level key-value options
    configuration.setString("table.exec.mini-batch.enabled", "true") // enable mini-batch optimization
    configuration.setString("table.exec.mini-batch.allow-latency", "5 s") // use 5 seconds to buffer input records
    configuration.setString("table.exec.mini-batch.size", "5000")

    val test_sql =
      """
        | CREATE TABLE test (
        |  id INT,
        |  name STRING,
        |  age INT,
        |  is_man int,
        |  salary_amt DECIMAL(10, 5),
        |  create_time TIMESTAMP(0)
        |) WITH (
        |  'connector' = 'mysql-cdc',
        |  'hostname' = 'hadoop001',
        |  'port' = '3306',
        |  'username' = 'root',
        |  'password' = 'linzhy123456!@',
        |  'database-name' = 'test_db',
        |  'table-name' = 'test'
        |)
        |
        |""".stripMargin

    val jdbc_sql =
      """
        | create table test2 (
        |    id int ,
        |    age int
        | ) with (
        |  'connector' = 'mysql-cdc',
        |  'hostname' = 'hadoop001',
        |  'port' = '3306',
        |  'username' = 'root',
        |  'password' = 'linzhy123456!@',
        |  'database-name' = 'test_db',
        |  'table-name' = 'test2'
        | )
        |
        |
        |""".stripMargin

    val sql2 =
      """
        | create table test3 (
        |    id int ,
        |    age int,
        |    name string,
        |    PRIMARY KEY (id) NOT ENFORCED
        | ) with (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://hadoop001:3306/test_db',
        |   'table-name' = 'test3',
        |   'username' = 'root',
        |   'password' = 'linzhy123456!@',
        |   'driver' = 'com.mysql.jdbc.Driver',
        |   'lookup.cache.max-rows' = '500',
        |   'lookup.cache.ttl' = '3s',
        |   'lookup.max-retries' = '3',
        |   'sink.buffer-flush.max-rows'='100',
        |   'sink.buffer-flush.interval'= '1s'
        | )
        |
        |""".stripMargin

    val  sql3 =
      """
        |
        | insert into test3 (id,age,name )
        | select
        |   a.id,
        |   b.age,
        |   a.name
        | from
        |   test a
        | left join test2 b on a.id = b.id
        |
        |""".stripMargin
    tableEnv.executeSql(test_sql)
    tableEnv.executeSql(jdbc_sql)
    tableEnv.executeSql(sql2)
    tableEnv.executeSql(sql3)


  }

}
