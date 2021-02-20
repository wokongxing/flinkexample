package com.xiaolin.flink.sink;

import com.xiaolin.flink.utils.KafkaUtil;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;
/**
 * @program: flinkexample
 * @description:
 * @author: linzy
 * @create: 2021-02-19 15:30
 **/
public class StreamingFileSinkApp {

    public static void main(String[] args) throws Exception {

        System.setProperty("HADOOP_USER_NAME","hdfs");

        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setParallelism(1);
        senv.enableCheckpointing(10 * 1000);

        /*指定source*/
//        DataStream source = senv.addSource().setParallelism(1);
        /*自定义滚动策略*/
//        DefaultRollingPolicy<String> rollPolicy = DefaultRollingPolicy.builder()
//                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))/*每隔多长时间生成一个文件*/
//                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))/*默认60秒,未写入数据处于不活跃状态超时会滚动新文件*/
//                .withMaxPartSize(128 * 1024 * 1024)/*设置每个文件的最大大小 ,默认是128M*/
//                .build();

        /*输出文件的前、后缀配置*/
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("prefix")
                .withPartSuffix(".txt")
                .build();

        StreamingFileSink streamingFileSink = StreamingFileSink
                /*forRowFormat指定文件的跟目录与文件写入编码方式，这里使用SimpleStringEncoder 以UTF-8字符串编码方式写入文件*/
                .forRowFormat(new Path("hdfs://192.168.0.101:8020/tmp/hdfsSink"), new SimpleStringEncoder("UTF-8"))
                /*这里是采用默认的分桶策略DateTimeBucketAssigner，它基于时间的分配器，每小时产生一个桶，格式如下yyyy-MM-dd--HH*/
                .withBucketAssigner(new DateTimeBucketAssigner<>())
                /*设置上面指定的滚动策略*/
//                .withRollingPolicy(rollPolicy)
                /*桶检查间隔，这里设置为1s*/
                .withBucketCheckInterval(1)
                /*指定输出文件的前、后缀*/
                .withOutputFileConfig(config)
                .build();
        /*指定sink*/
//        source.addSink(streamingFileSink);
        /*启动执行*/
        senv.execute("StreamingFileSinkTest");
    }

}
