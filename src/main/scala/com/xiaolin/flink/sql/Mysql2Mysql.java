package com.xiaolin.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-09-09 17:36
 **/
public class Mysql2Mysql {

    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql("create table project2 ( " +
                "        `id` BIGINT,\n" +
                "        `pid` BIGINT,\n" +
                "        `project_name` STRING,\n" +
                "        `city_code` STRING,\n" +
                "        `county_code` STRING,\n" +
                "        `create_time` timestamp(3),\n" +
                "         WATERMARK FOR create_time AS create_time - INTERVAL '2' SECOND,\n" +
                "         PRIMARY KEY (id) NOT ENFORCED" +
                "        )with ( " +
                "        'connector' = 'jdbc',\n" +
                "         'url' = 'jdbc:mysql://192.168.1.59:3306/salary_payment_temp_new',\n" +
                "         'username' = 'linzhongyan',\n" +
                "         'password' = 'Hw123456789#',\n"     +
                "         'table-name' = 'project2',\n" +
                "         'driver' = 'com.mysql.jdbc.Driver',\n" +
                "         'lookup.cache.max-rows' = '500',\n" +
                "         'lookup.cache.ttl' = '3s',\n" +
                "         'lookup.max-retries' = '3')"
        );

        tableEnvironment.executeSql("create table project1 ( " +
                "        `id` BIGINT,\n" +
                "        `pid` BIGINT,\n" +
                "        `project_name` STRING,\n" +
                "        `city_code` STRING,\n" +
                "        `county_code` STRING,\n" +
                "        `create_time` timestamp(3)" +
                "        )with ( " +
                "        'connector' = 'jdbc',\n" +
                "         'url' = 'jdbc:mysql://192.168.1.59:3306/salary_payment_temp_new',\n" +
                "         'username' = 'linzhongyan',\n" +
                "         'password' = 'Hw123456789#',\n"     +
                "         'table-name' = 'project1',\n" +
                "         'driver' = 'com.mysql.jdbc.Driver',\n" +
                "         'lookup.cache.max-rows' = '500',\n" +
                "         'lookup.cache.ttl' = '3s',\n" +
                "         'lookup.max-retries' = '3')"
        );

        tableEnvironment.executeSql("insert into  project1 SELECT id,pid,project_name,city_code,county_code,create_time from project2 " );

        try {
            environment.execute("ce");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
