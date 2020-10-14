package com.xiaolin.flink.connect;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-09-15 14:21
 **/
public class ConnectApp {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

       tEnv.sqlQuery("CREATE TABLE print_table (\n" +
                " f0 INT,\n" +
                " f1 INT,\n" +
                " f2 STRING,\n" +
                " f3 DOUBLE\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
                ")"
        );

    }
}
