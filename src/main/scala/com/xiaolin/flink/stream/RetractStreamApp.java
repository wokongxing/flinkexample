package com.xiaolin.flink.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-08-31 16:36
 **/
public class RetractStreamApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<PageVisit> input = env.fromElements(
                new PageVisit("2017-09-16 09:00:00", 1001, "/page1"),
                new PageVisit("2017-09-16 09:00:00", 1001, "/page2"),

                new PageVisit("2017-09-16 09:04:00", 1001, "/page1"),
                new PageVisit("2017-09-16 09:05:00", 1001, "/page2"),

                new PageVisit("2017-09-16 09:05:00", 1002, "/page2"),
                new PageVisit("2017-09-16 09:05:00", 1002, "/page1"),

                new PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
                new PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
                new PageVisit("2017-09-16 10:30:00", 1005, "/page2"));

        // register the DataStream as table "visit_table"
        tEnv.registerDataStream("visit_table", input, "visitTime, userId, visitPage");

//        Table table = tEnv.sqlQuery(
//                "SELECT " +
//                        "visitTime, " +
//                        "DATE_FORMAT(max(visitTime), 'HH') as ts, " +
//                        "count(userId) as pv, " +
//                        "count(distinct userId) as uv " +
//                        "FROM visit_table " +
//                        "GROUP BY visitTime"
//        );
        Table table =  tEnv.sqlQuery("select " +
                "visitTime," +
                "count(distinct userId ) uv," +
                " count(distinct userId) filter(where userId=1005) as suv" +
                " from visit_table" +
                " group by visitTime"
        );

        DataStream<Tuple2<Boolean, Row>> dataStream = tEnv.toRetractStream(table, Row.class);

        System.out.println("Printing result to stdout. Use --output to specify output path.");
        dataStream.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Row> value) throws Exception {
                return value.f0;
            }
        }).addSink(new RichSinkFunction<Tuple2<Boolean, Row>>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }
        });

        env.execute();
    }

    /**
     * Simple POJO containing a website page visitor.
     */
    public static class PageVisit {
        public String visitTime;
        public long userId;
        public String visitPage;

        // public constructor to make it a Flink POJO
        public PageVisit() {
        }

        public PageVisit(String visitTime, long userId, String visitPage) {
            this.visitTime = visitTime;
            this.userId = userId;
            this.visitPage = visitPage;
        }

        @Override
        public String toString() {
            return "PageVisit " + visitTime + " " + userId + " " + visitPage;
        }
    }
}
