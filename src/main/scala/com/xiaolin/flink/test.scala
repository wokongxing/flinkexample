package com.xiaolin.flink

import scala.collection.mutable

object test {
  def main(args: Array[String]): Unit = {
    val domains = Array("xiaolin.com","xiaoxiao.com","xiaozi.com")
    var userDomainMap = mutable.HashMap[String, String]()
    val map2 = Map("111" -> "xiaolin.com",
              "222" -> "xiaoxiao.com",
              "333" -> "xiaozi.com")
    print(map2.getOrElse("111",-99))
  }
}
