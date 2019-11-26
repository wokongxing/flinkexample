package com.xiaolin.flink.flink03.core

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.collection.mutable

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  * 读取MySQL中的数据
  */
class UserDomainMySQLSource extends SourceFunction[mutable.HashMap[String,String]]{

  var connection:Connection = _
  var pstmt:PreparedStatement = _



  override def cancel(): Unit = {

  }

  override def run(ctx: SourceFunction.SourceContext[mutable.HashMap[String, String]]): Unit = {

    val map = mutable.HashMap[String,String]()

    //connection = MySQLUtils.getConnection()
    pstmt = connection.prepareStatement("select user_id , domain from user_domains")
    val rs = pstmt.executeQuery()
    while(rs.next()) {
      val userId = rs.getString("user_id").trim
      val domain = rs.getString("domain").trim
      map.put(domain, userId)
    }

    if(map.size > 0) {
      ctx.collect(map)
    } else {
      println("从MySQL中获取数据为空...")  // TODO... log4j
    }
    //MySQLUtils.closeConnection(connection, pstmt)
  }
}
