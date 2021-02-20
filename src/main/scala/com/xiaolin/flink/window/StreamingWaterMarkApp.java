package com.xiaolin.flink.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

import java.time.Duration;

/**
 * @program: flinkexample
 * @description:
 * @author: linzy
 * @create: 2021-01-22 13:52
 **/
public class StreamingWaterMarkApp {

    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(1);

        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stream = environment.socketTextStream("hadoop001", 9999);
        //单调递增生成水印
//        WatermarkStrategy.forMonotonousTimestamps();
        stream.assignTimestampsAndWatermarks(WatermarkStrategy.noWatermarks());

        stream.assignTimestampsAndWatermarks(WatermarkStrategy
                .forBoundedOutOfOrderness(Duration.ofSeconds(20))
        );





    }
}
