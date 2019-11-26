package com.xiaolin.flink.utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConverters._
import scala.collection.mutable

object HbaseUtil {
  var conf: Configuration = _
  //线程池
  lazy val connection: Connection = ConnectionFactory.createConnection(conf)
  lazy val admin: Admin = connection.getAdmin

  /**
    * hbase conf
    * @param quorum hbase的zk地址
    * @param port   zk端口2181
    * @return
    */
  def setConf(quorum: String, port: String): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)

    this.conf = conf
  }
  def setConf(quorum: String, port: String,
              rpctimeout:String="2000",optimeout:String="3000",scantimeout:String="10000"): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", quorum)
    conf.set("hbase.zookeeper.property.clientPort", port)
    conf.set("hbase.rpc.timeout", rpctimeout)
    conf.set("hbase.client.operation.timeout", optimeout)
    conf.set("hbase.client.scanner.timeout.period", scantimeout)

    this.conf = conf
  }

  /**
    * 如果不存在就创建表
    * @param tableName 命名空间：表名
    * @param columnFamily 列族
    */
  def createTable(tableName: String, columnFamily: String): Unit = {
    val tbName = TableName.valueOf(tableName)
    if (!admin.tableExists(tbName)) {
      val htableDescriptor = new HTableDescriptor(tbName)
      val hcolumnDescriptor = new HColumnDescriptor(columnFamily)
      htableDescriptor.addFamily(hcolumnDescriptor)
      admin.createTable(htableDescriptor)
    }
  }

  def hbaseScan(tableName: String): ResultScanner = {
    val scan = new Scan()
//    scan.setStartRow(Bytes.toBytes("%1%"))
//    scan.setStopRow(Bytes.toBytes("%3%"))
    val table = connection.getTable(TableName.valueOf(tableName))
    table.getScanner(scan)
    //    val scanner: CellScanner = rs.next().cellScanner()
  }

  /**
    * 获取hbase单元格内容
    * @param tableName 命名空间：表名
    * @param rowKey rowkey
    * @return 返回单元格组成的List
    */
  def getCell(tableName: String, rowKey: String): mutable.Buffer[Cell] = {
    val get = new Get(Bytes.toBytes(rowKey))

    val table = connection.getTable(TableName.valueOf(tableName))
    val result: Result = table.get(get)
    result.listCells().asScala

  }

  /**
    * 单条插入
    * @param tableName 命名空间：表名
    * @param rowKey rowkey
    * @param family 列族
    * @param qualifier column列
    * @param value 列值
    */
  def singlePut(tableName: String, rowKey: String, family: String, qualifier: String, value: String): Unit = {
    //向表中插入数据//向表中插入数据
    //a.单个插入
    val put: Put = new Put(Bytes.toBytes(rowKey)) //参数是行健row01
    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
    //获得表对象
    val table: Table = connection.getTable(TableName.valueOf(tableName))
    table.put(put)
    table.close()
  }
  /**
    * 删除数据
    * @param tbName 表名
    * @param row rowkey
    */
  def deleteByRow(tbName:String,row:String): Unit ={
    val delete = new Delete(Bytes.toBytes(row))
    //    delete.addColumn(Bytes.toBytes("fm2"), Bytes.toBytes("col2"))
    val table = connection.getTable(TableName.valueOf(tbName))
    table.delete(delete)
  }

  def close(): Unit = {
    admin.close()
    connection.close()
  }


  def main(args: Array[String]): Unit = {

    setConf("hadoop001", "2181")

//    val cells = getCell("kafka_offset:grampus_double_groupid", "grampus_erp")
//    cells.foreach(cell => {
//      val rowKey = Bytes.toString(CellUtil.cloneRow(cell))
//      val timestamp = cell.getTimestamp; //取到时间戳
//      val family = Bytes.toString(CellUtil.cloneFamily(cell)) //取到族列
//      val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell)) //取到修饰名
//      val value = Bytes.toString(CellUtil.cloneValue(cell))
//      println(rowKey, timestamp, family, qualifier, value)
//    })

    val scans = hbaseScan("order_sku").iterator()
    while (scans.hasNext){
      val data = scans.next()
      print(data.value())
      val rowKey = Bytes.toString(data.getRow)
      val sb: StringBuffer = new StringBuffer()

      for (cell:Cell <- data.listCells().asScala){
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
        sb.append(value).append("_")
      }

      println(rowKey+"::::"+sb)

    }
    this.close()
  }

}
