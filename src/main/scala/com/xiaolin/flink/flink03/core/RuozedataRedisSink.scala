package com.xiaolin.flink.flink03.core

import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * 若泽数据  www.ruozedata.com
  * 讲师：PK哥   
  * 交流群：545916944
  */
class RuozedataRedisSink extends RedisMapper[(String,Double)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "ruozedata_traffic")
  }

  override def getValueFromData(data: (String, Double)): String = {
    data._2 + ""
  }

  override def getKeyFromData(data: (String, Double)): String = {
    data._1
  }
}
