package com.xiaolin.flink.source

import com.xiaolin.flink.entity.Student
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  * 读取MySQL中的数据
  */
class ScalikeJDBCMySQLSource extends RichSourceFunction[Student]{

  override def cancel(): Unit = {

  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    DBs.setupAll()  // parse configuration file

    DB.readOnly{ implicit session => {
      SQL("select * from student").map(rs => {
       // val student = Student(rs.int("id"), rs.string("name"), rs.int("age"))
        //ctx.collect(student)
      }).list().apply()
    }

    }

  }
}
