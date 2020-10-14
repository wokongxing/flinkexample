package com.xiaolin.flink.join;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @program: flink-example
 * @description: 使用open函数 预加载维表数据到缓存中 维表join 实时
 * @author: linzy
 * @create: 2020-08-28 17:48
 **/
public class DimJoinJavaApi {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Joinobject> stream1 = env.addSource(new RichSourceFunction<Joinobject>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<Joinobject> ctx) throws Exception {
                Joinobject joinobject = new Joinobject();
                while (isRunning) {

                    Thread.sleep(1000);
                    Random random = new Random();
                    int i = random.nextInt(5);

                    joinobject.setDomain(i);
                    joinobject.setValue(i);
                    joinobject.setRemark("流1");
                    System.out.println("流1生成数据:" + joinobject.toString());
                    ctx.collect(joinobject);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });

//        //来一条数据查询一条数据
//        stream1.map(new RichMapFunction<Joinobject, Object>() {
//            private Connection connection = null;
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //预加载相关维表到缓存中
//                Class.forName("com.mysql.jdbc.Driver");  //注册数据库驱动
//                connection = DriverManager.getConnection(
//                        "jdbc:mysql://192.168.8.181:3306/niu?characterEncoding=UTF-8",
//                        "root",
//                        "123456"
//                );
//            }
//            @Override
//            public Object map(Joinobject value) throws Exception {
//                PreparedStatement preparedStatement = connection.prepareStatement("select * from test where id=?");
//                preparedStatement.setInt(1,value.domain);
//                ResultSet resultSet = preparedStatement.executeQuery();
//                return new Tuple2<>(value,resultSet.getString(1));
//
//            }
//        });

//        //预加载缓存实时关联
//        stream1.map(new RichMapFunction<Joinobject, Object>() {
//            //缓存数据
//            private Map<String, String> cache = new HashMap<String, String>();
//            @Override
//            public void open(Configuration parameters) throws Exception {
//                //预加载相关维表到缓存中
//                Class.forName("com.mysql.jdbc.Driver");  //注册数据库驱动
//                Connection connection = DriverManager.getConnection(
//                        "jdbc:mysql://192.168.8.181:3306/niu?characterEncoding=UTF-8",
//                        "root",
//                        "123456"
//                );
//                //初始化0，等上次任务完成后，等待10秒执行本次任务  定义了一个10秒的定时器，定时执行查询数据库的方法
//                Executors.newScheduledThreadPool(3).scheduleWithFixedDelay(new Runnable() {
//                       @Override
//                       public void run() {
//                           try {
//                               //根据isa 查询 vin
//                               PreparedStatement pst = connection.prepareStatement("select * from test");
//                               ResultSet resultSet = pst.executeQuery();
//                               while (resultSet.next()) {
//                                   String id = resultSet.getString("id");
//                                   String value = resultSet.getString("value");
//                                   cache.put(id, value);
//                               }
//                               pst.close();
//                           } catch (SQLException e) {
//                               e.printStackTrace();
//                           }finally {
//                               try {
//                                   connection.close();
//                               } catch (SQLException e) {
//                                   e.printStackTrace();
//                               }
//                           }
//                       }
//                   },
//                0,     //初始化时间
//                10,    //任务间隔时间
//                TimeUnit.MILLISECONDS); //时间单位
//
//            }
//            @Override
//            public Object map(Joinobject value) throws Exception {
//                return new Tuple2<>(value,cache.getOrDefault(value.domain,"无关联数据"));
//            }
//        });
//


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
