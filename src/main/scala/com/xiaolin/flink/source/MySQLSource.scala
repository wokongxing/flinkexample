package com.xiaolin.flink.source

import java.sql.{Connection, PreparedStatement}

import com.xiaolin.flink.entity.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  * 读取MySQL中的数据
  */
class MySQLSource extends RichSourceFunction[Student]{

  var connection:Connection = _
  var pstmt:PreparedStatement = _

  // 在open方法中建立连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    //connection = MySQLUtils.getConnection()
    pstmt = connection.prepareStatement("select * from student")
  }

  // 释放
  override def close(): Unit = {
    super.close()
    //MySQLUtils.closeConnection(connection, pstmt)
  }

  override def cancel(): Unit = {

  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val rs = pstmt.executeQuery()
    while(rs.next()){
      //val student = Student(rs.getInt("id"), rs.getString("name"), rs.getInt("age"))
     // ctx.collect(student)
    }
  }
}
