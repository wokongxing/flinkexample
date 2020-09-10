package com.xiaolin.flink.sink

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
class RuozedataMySQLSink extends RichSinkFunction[(String,Double)]{

  var connection:Connection = _
  var insertPstmt:PreparedStatement = _
  var updatePstmt:PreparedStatement = _

  // 打开connection等
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    //connection = MySQLUtils.getConnection()
    insertPstmt = connection.prepareStatement("insert into ruozedata_traffic(domain,traffic) values (?,?)")
    updatePstmt = connection.prepareStatement("update ruozedata_traffic set traffic=? where domain=?")
  }

  /**
    * 写数据
    */
  override def invoke(value: (String, Double), context: SinkFunction.Context[_]): Unit = {
    // TODO   insert update

    updatePstmt.setDouble(1, value._2)
    updatePstmt.setString(2, value._1)
    updatePstmt.execute()

    if(updatePstmt.getUpdateCount == 0) {
      insertPstmt.setString(1, value._1)
      insertPstmt.setDouble(2, value._2)
      insertPstmt.execute()
    }
  }


  // 释放资源
  override def close(): Unit = {
    super.close()
    if(insertPstmt != null) insertPstmt.close()
    if(updatePstmt != null) updatePstmt.close()
    if(connection != null) connection.close()
  }

}
