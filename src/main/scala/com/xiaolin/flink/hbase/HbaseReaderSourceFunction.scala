package com.xiaolin.flink.hbase

import com.xiaolin.flink.utils.HbaseUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.hadoop.hbase.{Cell, TableName}
import org.apache.hadoop.hbase.client.{Connection, Scan, Table}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._
class HbaseReaderSourceFunction extends RichSourceFunction[(String,String)] {

  private var conn: Connection = _
  private var table: Table = _

  override def open(parameters: Configuration): Unit = {
    HbaseUtil.setConf("hadoop001","2181")
    conn = HbaseUtil.connection
    table = conn.getTable(TableName.valueOf("order_sku"))
  }

  override def close(): Unit = {
    HbaseUtil.close()
  }
  override def run(ctx: SourceFunction.SourceContext[(String,String)]): Unit = {
      val data = table.getScanner(new Scan).iterator()
    while (data.hasNext){
      val res = data.next()
      val rowKey = Bytes.toString(res.getRow)
      val sb: StringBuffer = new StringBuffer()
      for (cell:Cell <- res.listCells().asScala){
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        sb.append(value).append(",")
      }
      ctx.collect((rowKey,sb.toString))
    }


  }

  override def cancel(): Unit = {

  }
}
