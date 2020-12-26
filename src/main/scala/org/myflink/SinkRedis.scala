package org.myflink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis._
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper._

object SinkRedis {
  def main(args:Array[String]): Unit ={
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    val fileStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    var stream = fileStream.map((x)=>{
      val y = x.split(",")
      Sensor5(y(0), y(1).toLong, y(2).toDouble)
    })
    stream.print
    val conf = new FlinkJedisPoolConfig.Builder()
      .setHost("10.227.20.135")
      .setPort(6379)
      .build()

    stream.addSink(new RedisSink[Sensor5](conf, new MyRedisWrapper))

    env.execute("redis sink")
  }
}
case class Sensor5(id:String, timestamp:Long, temp:Double)
class MyRedisWrapper extends RedisMapper[Sensor5] {
  override def getCommandDescription: RedisCommandDescription =
    new RedisCommandDescription(RedisCommand.HSET, "temp")

  override def getKeyFromData(t: Sensor5): String = t.id

  override def getValueFromData(t: Sensor5): String = t.temp.toString
}