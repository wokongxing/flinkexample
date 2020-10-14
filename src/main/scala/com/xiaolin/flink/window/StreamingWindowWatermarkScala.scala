package com.xiaolin.flink.window

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
 * @program: flink-example
 * @description:
 * flink,1593421135000
 * flink,1593421136000
 * flink,1593421137000
 * flink,1593421138000
 * flink,1593421142000
 * flink,1593421145000
 * flink,1593421146000
 * flink,1593421147000
 * flink,1593421148000
 * flink,1593421134000
 * flink,1593421135000
 *
 * currentThreadId:76,key:flink,eventtime:[1593421135000|2020-06-29 16:58:55.000],currentMaxTimestamp:[1593421135000|2020-06-29 16:58:55.000],watermark:[1593421125000|2020-06-29 16:58:45.000]
 * currentThreadId:76,key:flink,eventtime:[1593421136000|2020-06-29 16:58:56.000],currentMaxTimestamp:[1593421136000|2020-06-29 16:58:56.000],watermark:[1593421126000|2020-06-29 16:58:46.000]
 * currentThreadId:76,key:flink,eventtime:[1593421137000|2020-06-29 16:58:57.000],currentMaxTimestamp:[1593421137000|2020-06-29 16:58:57.000],watermark:[1593421127000|2020-06-29 16:58:47.000]
 * currentThreadId:76,key:flink,eventtime:[1593421138000|2020-06-29 16:58:58.000],currentMaxTimestamp:[1593421138000|2020-06-29 16:58:58.000],watermark:[1593421128000|2020-06-29 16:58:48.000]
 * currentThreadId:76,key:flink,eventtime:[1593421142000|2020-06-29 16:59:02.000],currentMaxTimestamp:[1593421142000|2020-06-29 16:59:02.000],watermark:[1593421132000|2020-06-29 16:58:52.000]
 * currentThreadId:76,key:flink,eventtime:[1593421145000|2020-06-29 16:59:05.000],currentMaxTimestamp:[1593421145000|2020-06-29 16:59:05.000],watermark:[1593421135000|2020-06-29 16:58:55.000]
 * currentThreadId:76,key:flink,eventtime:[1593421146000|2020-06-29 16:59:06.000],currentMaxTimestamp:[1593421146000|2020-06-29 16:59:06.000],watermark:[1593421136000|2020-06-29 16:58:56.000]
 * currentThreadId:76,key:flink,eventtime:[1593421147000|2020-06-29 16:59:07.000],currentMaxTimestamp:[1593421147000|2020-06-29 16:59:07.000],watermark:[1593421137000|2020-06-29 16:58:57.000]
 *
 * @author: linzy
 * @create: 2020-09-22 15:43
 **/
object StreamingWindowWatermarkScala {

  def main(args: Array[String]): Unit = {
    // socket 的端口号
    val port = 9000
    // 运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    // 使用EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并行度为 1,默认并行度是当前机器的 cpu 数量,多并行度时 只有各个节点的watermark均符合条件时才触发窗口计算
    env.setParallelism(2)

    val text = env.socketTextStream("47.114.92.31", port, '\n')

    //解析输入的数据
    val inputMap = text.map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toLong)
    })


    // 抽取 timestamp 和生成 watermark,,将窗口的触发时间延后10s
    val waterMarkStream = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L
      var maxOutOfOrderness = 10000L // 最大允许的乱序时间是10s

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      // 定义生成 watermark 的逻辑, 默认 100ms 被调用一次
      // 当前最大的时间点 - 允许的最大时间
      override def getCurrentWatermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness)


      // 提取 timestamp
      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val timestamp = element._2
        // 这里想象一个迟到的数据时间，所以这里得到的是当前数据进入的最大时间点
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        val id = Thread.currentThread().getId
        println("currentThreadId:" + id + ",key:" + element._1 + ",eventtime:[" + element._2 + "|" + sdf.format(element._2) + "],currentMaxTimestamp:[" + currentMaxTimestamp + "|" + sdf.format(currentMaxTimestamp) + "],watermark:[" + getCurrentWatermark().getTimestamp + "|" + sdf.format(getCurrentWatermark().getTimestamp) + "]")
        timestamp
      }
    })


    // 保存被丢弃的数据, 定义一个 outputTag 来标识
        val outputTag = new OutputTag[Tuple2[String, Long]]("late-data") {}

    // 分组, 聚合
    val window = waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))   //按照消息的EventTime分配窗口，和调用TimeWindow效果一样
            .allowedLateness(Time.seconds(2))   //在 WaterMark 基础上还可以延迟2s, 即：数据迟到 2s
            .sideOutputLateData(outputTag)
      .apply(new WindowFunction[(String, Long), String, Tuple, TimeWindow] {

        /**
         * Evaluates the window and outputs none or several elements.
         *
         * @param key    The key for which this window is evaluated.
         * @param window The window that is being evaluated.
         * @param input  The elements in the window being evaluated.
         * @param out    A collector for emitting elements.
         * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
         *
         * 对 window 内的数据进行排序，保证数据的顺序
         */
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
          println("key值：", key)
          val keyStr = key.toString
          val arrBuf = ArrayBuffer[Long]()
          val ite = input.iterator
          while (ite.hasNext) {
            val tup2 = ite.next()
            arrBuf.append(tup2._2)
          }

          val arr = arrBuf.toArray
          Sorting.quickSort(arr)

          val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
          val result = keyStr + "," + arr.length + ", data_range[" +
            sdf.format(arr.head) + "," + sdf.format(arr.last) + "], window [" +       // 数据的开始时间 和 结束时间
            sdf.format(window.getStart) + "," + sdf.format(window.getEnd) + ")"   // 该窗口的开始时间 和 结束时间[) -> 记得左闭右开
          out.collect(result)
        }
      })

    // 侧输出流
        val sideOutput: DataStream[Tuple2[String, Long]] = window.getSideOutput(outputTag)
        sideOutput.print("side_lated_data")

    window.print()

    // 因为 flink 默认懒加载, 所以必须调用 execute 方法, 上面的代码才会执行
    env.execute("StreamingWindowWatermarkScala")

  }
}
