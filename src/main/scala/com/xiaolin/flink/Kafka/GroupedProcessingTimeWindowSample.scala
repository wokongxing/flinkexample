package com.xiaolin.flink.Kafka
  import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
  import org.apache.flink.streaming.api.functions.sink.SinkFunction
  import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
  import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

  import scala.collection.mutable
  import scala.util.Random

/** 实时订单统计示例
 * @author linzhy
 */
  object GroupedProcessingTimeWindowSample {

    private class DataSource extends RichParallelSourceFunction[Tuple2[String,Int]] {
      private var isRunning = true

      override def run(ctx: SourceContext[Tuple2[String,Int]]):Unit = {
        val random = new Random()

        while (isRunning) {
          Thread.sleep((getRuntimeContext.getIndexOfThisSubtask + 1) * 1000 * 5)
          val key = "类别" + ('A' + random.nextInt(3)).toString
          val value = random.nextInt(10) + 1
          ctx.collect(Tuple2(key, value))

        }
      }

      override def cancel(): Unit =
        isRunning = false
    }

    @throws[Exception]
    def main(args: Array[String]): Unit = {

      val env = StreamExecutionEnvironment.getExecutionEnvironment
      env.setParallelism(2)
      val value = env.addSource(new GroupedProcessingTimeWindowSample.DataSource)
      val keyedStream = value.keyBy(0)

      keyedStream.sum(1).keyBy(0)
//        .fold(new mutable.HashMap[String, Int], new FoldFunction[Tuple2[String,Int], mutable.HashMap[String, Int]]() {
//
//          @throws[Exception]
//          override def fold(accumulator: mutable.HashMap[String, Int], value: Tuple2[String,Int]): mutable.HashMap[String, Int] = {
//            accumulator += (value._1->value._2)
//          }
//
//      }).addSink(new SinkFunction[mutable.HashMap[String, Int]]() {
//
//          @throws[Exception]
//          override def invoke(value: mutable.HashMap[String, Int], context: SinkFunction.Context[_]): Unit = { // 每个类型的商品成交量
//            //商品分类数量
//            println(value)
//            // 商品成交总量
//            println(value)
//          }
//      })
      env.execute
    }
  }

