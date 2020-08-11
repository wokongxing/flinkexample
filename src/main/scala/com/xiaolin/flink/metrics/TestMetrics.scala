package com.xiaolin.flink.metrics

import org.apache.flink.api.common.functions.{IterationRuntimeContext, RichFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration

object TestMetrics extends RichFunction{
  override def open(parameters: Configuration): Unit = ???

  override def close(): Unit = ???

  override def getRuntimeContext: RuntimeContext = ???

  override def getIterationRuntimeContext: IterationRuntimeContext = ???

  override def setRuntimeContext(t: RuntimeContext): Unit = {
    val counter = t.getMetricGroup.counter("processed_count")
    t.getMetricGroup.addGroup(1)

  }
}
