package com.xiaolin.flink.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.optimizer.operators.MapDescriptor;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.shaded.curator4.com.google.common.collect.Streams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.runtime.util.StateTtlConfigUtil;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-09-04 17:06
 **/
public class HotWordApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setParallelism(1);
        ArrayList<String> list = new ArrayList<>();
        list.add("小李");
        list.add("小王");
        list.add("小林");
        list.add("小刀");

        ArrayList<Long> time = new ArrayList<>();
        time.add(1000L);
        time.add(2000L);
        time.add(3000L);
        time.add(4000L);

        DataStreamSource<Tuple2<String,Long>> streamSource = streamExecutionEnvironment.addSource(new RichSourceFunction<Tuple2<String,Long>>() {
            private boolean isruning=true;
            @Override
            public void run(SourceContext<Tuple2<String,Long>> ctx) throws Exception {
                Random random = new Random();
                while (isruning){

                    int i = random.nextInt(4);

                    ctx.collect(new Tuple2<String,Long>(list.get(i), 1L) );
                    Thread.sleep(5000);
                }

            }

            @Override
            public void cancel() {
                isruning=false;
            }
        });
        streamSource.keyBy(new KeySelector<Tuple2<String,Long>, Object>() {

            @Override
            public Object getKey(Tuple2<String,Long> value) throws Exception {
                return value.f0;
            }
        }).process(new KeyedProcessFunction<Object, Tuple2<String,Long>, Object>() {
            private MapState<String, Long> keywordState;

            @Override
            public void open(Configuration parameters) throws Exception {
                MapStateDescriptor<String, Long> keywordmapDescriptor = new MapStateDescriptor<>("keyword", String.class, Long.class);
                StateTtlConfig ttlConfig = StateTtlConfigUtil.createTtlConfig(1000000L);
                keywordmapDescriptor.enableTimeToLive(ttlConfig);
                keywordState = getRuntimeContext().getMapState(keywordmapDescriptor);

            }

            @Override
            public void processElement(Tuple2<String,Long> value, Context ctx, Collector<Object> out) throws Exception {
                    if(keywordState.get(value.f0.toString())!=null){

                        keywordState.put(value.f0.toString(),keywordState.get(value.f0.toString()).longValue()+1L);
                    }else{
                        keywordState.put(value.f0.toString(),1L);
                    }

                long coalescedTime = ((System.currentTimeMillis() + 5000) / 5000) * 5000;
                ctx.timerService().registerProcessingTimeTimer(coalescedTime);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            if (!keywordState.isEmpty()){
                keywordState.entries().forEach(x->{
                    String key = x.getKey();
                    Long value = x.getValue();
                    System.out.println(System.currentTimeMillis()+"----------------------------");
                    out.collect(new Tuple2<>(key,value));
                });
//                keywordState.clear();
            }

            }

        }).print()
        ;
        try {
            streamExecutionEnvironment.execute("cese");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
