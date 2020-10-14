package com.xiaolin.flink.stream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-09-14 14:16
 **/
public class AppendOnlyApp {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        DataStream<Tuple2<String, String>> data = env.fromElements(
                new Tuple2<>("Mary", "./home"),
                new Tuple2<>("Bob", "./cart"),
                new Tuple2<>("Mary", "./prod?id=1"),
                new Tuple2<>("Liz", "./home"),
                new Tuple2<>("Bob", "./prod?id=3")
        );

        Table clicksTable = tEnv.fromDataStream(data, "user,url");

        tEnv.registerTable("clicks", clicksTable);
        Table rTable = tEnv.sqlQuery("select user,url from clicks where user='Mary'");

        DataStream ds = tEnv.toAppendStream(rTable, TypeInformation.of(new TypeHint<Tuple2<String, String>>(){}));
        ds.print();

//        (Mary,./home)
//        (Mary,./prod?id=1)
        env.execute();
    }
}
