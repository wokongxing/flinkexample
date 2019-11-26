package com.xiaolin.flink.flink03

import org.apache.flink.streaming.api.functions.source.SourceFunction
import scalikejdbc.{DB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable

class MysqlSourceApp extends SourceFunction[mutable.HashMap[String,String]] {

  override def run(sourceContext: SourceFunction.SourceContext[mutable.HashMap[String,String]]): Unit = {
    var map = new mutable.HashMap[String,String]()
    DBs.setupAll()

    val list = DB.readOnly(implicit session => {
      SQL("SELECT user_id,domain from user_domain").map(res => {
        val userid = res.string("user_id")
        val domain = res.string("domain")

        map.put(domain.trim,userid.trim)
      }).list().apply()
    })

    if(map.size > 0) {
      sourceContext.collect(map)
    } else {
      println("从MySQL中获取数据为空...")  // TODO... log4j
    }

  }

  override def cancel(): Unit = ???

}
