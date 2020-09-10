package com.xiaolin.flink.join;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import scala.Tuple1;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.mutable.HashMap;

import java.util.Properties;
import java.util.Random;

/**
 * @program: flink-example
 * @description:
 * @author: linzy
 * @create: 2020-08-28 17:48
 **/
public class JoinJavaApi {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Joinobject> stream1 = env.addSource(new RichSourceFunction<Joinobject>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<Joinobject> ctx) throws Exception {
                Joinobject joinobject = new Joinobject();
                while (isRunning){

                    Thread.sleep(1000);
                    Random random = new Random();
                    int i = random.nextInt(5);

                    joinobject.setDomain(i);
                    joinobject.setValue(i);
                    joinobject.setRemark("流1");
                    System.out.println("流1生成数据:"+joinobject.toString());
                    ctx.collect(joinobject);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

        DataStreamSource<Joinobject> stream2 = env.addSource(new RichSourceFunction<Joinobject>() {
            private volatile boolean isRunning = true;
            @Override
            public void run(SourceContext<Joinobject> ctx) throws Exception {
                Joinobject joinobject = new Joinobject();
                while (isRunning){

                    Thread.sleep(1000);
                    Random random = new Random();
                    int i = random.nextInt(10);

                    joinobject.setDomain(i);
                    joinobject.setValue(i);
                    joinobject.setRemark("流2");
                    System.out.println("流2生成数据:"+joinobject.toString());
                    ctx.collect(joinobject);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

//        stream1.connect(stream2).flatMap(new RichCoFlatMapFunction<Joinobject, Joinobject, Object>() {
//
//            @Override
//            public void flatMap1(Joinobject value, Collector<Object> out) throws Exception {
//                out.collect(new Tuple2("log1:"+value.domain+"--"+value.getValue(),000000000));
//            }
//
//            @Override
//            public void flatMap2(Joinobject value, Collector<Object> out) throws Exception {
//                out.collect(new Tuple1("log2:"+value.domain+"--"+value.getValue()));
//            }
//
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                super.open(parameters);
//            }
//
//        }).print();

//        stream1.connect(stream2).process(new KeyedCoProcessFunction<Object, Joinobject, Joinobject, Object>() {
//            @Override
//            public void processElement1(Joinobject value, Context ctx, Collector<Object> out) throws Exception {
//
//            }
//            @Override
//            public void processElement2(Joinobject value, Context ctx, Collector<Object> out) throws Exception {
//
//            }
//            @Override
//            public void open(Configuration parameters) throws Exception {
//
//            }
//        });
        //join 仅有在当前窗口内符合条件的数据输出
//        流2生成数据:Joinobject{domain=8, value=8, remark='流2'}
//        流1生成数据:Joinobject{domain=2, value=2, remark='流1'}
//        流1生成数据:Joinobject{domain=0, value=0, remark='流1'}
//        流2生成数据:Joinobject{domain=2, value=2, remark='流2'}
//        16> (2,流1流2)
//        stream1.join(stream2).where(new KeySelector<Joinobject, Object>() {
//
//            @Override
//            public Object getKey(Joinobject value) throws Exception {
//                return value.getDomain();
//            }
//        }).equalTo(new KeySelector<Joinobject, Object>() {
//
//            @Override
//            public Object getKey(Joinobject value) throws Exception {
//                return value.getDomain();
//            }
//        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .apply(new JoinFunction<Joinobject, Joinobject, Object>() {
//
//                    @Override
//                    public Object join(Joinobject first, Joinobject second) throws Exception {
//                        return new Tuple2(first.domain,first.remark+second.remark);
//                    }
//                }).print();
        //coGroup 不管双流是否符合 均输出 可通过first second 不同的比较,实现leftjoin rightjoin
//        stream1.coGroup(stream2).where(new KeySelector<Joinobject, Object>() {
//            @Override
//            public Object getKey(Joinobject value) throws Exception {
//                return value.getDomain();
//            }
//        }).equalTo(new KeySelector<Joinobject, Object>() {
//            @Override
//            public Object getKey(Joinobject value) throws Exception {
//                return value.getDomain();
//            }
//        }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .apply(new CoGroupFunction<Joinobject, Joinobject, Object>() {
//                    @Override
//                    public void coGroup(Iterable<Joinobject> first, Iterable<Joinobject> second, Collector<Object> out) throws Exception {
//                        StringBuffer stringBuffer = new StringBuffer();
//                        first.forEach(f->
//                                second.forEach(s->
//                                        {
//                                            int f_domain = f.getDomain();
//                                            String fRemark = f.getRemark();
//                                            int s_domain = s.getDomain();
//                                            String sRemark = s.getRemark();
//                                            stringBuffer.append(f_domain).append("f--s").append(s_domain)
//                                                    .append(":").append(fRemark).append("--").append(sRemark);
//                                        }
//                                        )
//                                );
//                        out.collect(stringBuffer);
//                    }
//                }).print();

        stream1.keyBy(new KeySelector<Joinobject, Object>() {

            @Override
            public Object getKey(Joinobject value) throws Exception {
                return value.getDomain();
            }
        }).process(new KeyedProcessFunction<Object, Joinobject, Object>() {

            @Override
            public void processElement(Joinobject value, Context ctx, Collector<Object> out) throws Exception {

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }
        });

        try {
            env.execute("join");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Joinobject{


        private int domain;
        private int value;
        private String remark;

        public void setRemark(String remark) {
            this.remark = remark;
        }

        public String getRemark() {
            return remark;
        }

        public int getDomain() {
            return domain;
        }

        public int getValue() {
            return value;
        }
        public void setDomain(int domain) {
            this.domain = domain;
        }

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Joinobject{" +
                    "domain=" + domain +
                    ", value=" + value +
                    ", remark='" + remark + '\'' +
                    '}';
        }
    }
}
