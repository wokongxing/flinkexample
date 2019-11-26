package com.xiaolin.flink.work.work01

import com.xiaolin.flink.utils.HbaseUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes

class HbaseWriterSinkFunction extends RichSinkFunction[String]{

  private var conn: Connection = _
  private var table: Table = _
  private var mutator: BufferedMutator = _

  override def open(parameters: Configuration): Unit = {
    HbaseUtil.setConf("hadoop001","2181")
    conn = HbaseUtil.connection
    val params: BufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf("ruoze"))

    //设置缓存1m，当达到1m时数据会自动刷到hbase
    params.writeBufferSize(1024 * 1024)

    mutator = conn.getBufferedMutator(params)

  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
      val cf = "user"
      val split = value.split(",")
      val rowkey = new StringBuffer(split(0)).reverse().toString+"_"+split(1)
      var count=0

      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("phone"),Bytes.toBytes(split(0)))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("name"),Bytes.toBytes(split(1)))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("age"),Bytes.toBytes(split(2)))
      put.addColumn(Bytes.toBytes(cf),Bytes.toBytes("sex"),Bytes.toBytes(split(3)))
      mutator.mutate(put)

      //if (count==2000){//满足2000条 刷新到hbase
        mutator.flush()
       // count=0
      //}

      count=count+1
  }

  override def close(): Unit = {
    HbaseUtil.close()
  }

}
