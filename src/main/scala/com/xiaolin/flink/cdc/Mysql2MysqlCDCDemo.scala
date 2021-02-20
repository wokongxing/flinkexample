package com.xiaolin.flink.cdc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @program: flinkexample
 * @description:
 * @author: linzy
 * @create: 2021-01-26 16:02
 **/
object Mysql2MysqlCDCDemo {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env);

    val test_sql =
      """
        | CREATE TABLE test_cdc (
        |  id INT,
        |  name STRING,
        |  age INT,
        |  is_man BOOLEAN,
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
        |);
        |
        |""".stripMargin

    tableEnv.executeSql(test_sql)


  }

}
