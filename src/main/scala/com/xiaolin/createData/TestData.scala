package com.xiaolin.createData

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.util
import java.util.ArrayList

import org.apache.commons.io.FileUtils

import scala.io.Source

class TestData {



}

//long timestamp() 生成当前13位时间戳（ms）。
//int intRand() 生成int正随机整数。
//int intRand(Integer n) 生成0～n的随机整数。
//int intRand(Integer s, Integer e) 生成s～e的随机整数。
//long longRand() 生成long正随机整数。
//double doubleRand() 生成0～1.0的随机双精度浮点数。
//doubleRand(Integer s, Integer e, Integer n) 生成s～e的保留n位有效数字的浮点数。
//String uuid() 生成一个uuid。
//String numRand(Integer n) 生成n位随机数。
//String strRand(Integer n) 生成n位字符串，包括数字，大小写字母

object TestData{

  def main(args: Array[String]): Unit = {
    import com.cloudwise.sdg.dic.DicInitializer
    import com.cloudwise.sdg.template.TemplateAnalyzer
    //加载词典(只需执行一次即可)//加载词典(只需执行一次即可)

    DicInitializer.init()
    //编辑模版
    val templates: File = new File("templates/ruoze.tpl")
    val tpl = FileUtils.readFileToString(templates)
    val tplName = templates.getName

    //创建模版分析器（一个模版new一个TemplateAnalyzer对象即可）
    val testTplAnalyzer: TemplateAnalyzer = new TemplateAnalyzer(tplName, tpl)
    //分析模版生成模拟数据
    val write=new PrintWriter("data/ruoze.log")

    for (i <-1 to 100){
      val abc: String = testTplAnalyzer.analyse
      //打印分析结果
      write.println(abc)
    }


    write.close()
  }



}