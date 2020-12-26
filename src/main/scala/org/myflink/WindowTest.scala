package org.myflink

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //var originStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    var originStream = env.socketTextStream("10.227.20.135", 7777)
    var stream = originStream
      .map((x)=>{
        val y = x.split(",")
        Sensor8(y(0), y(1).toLong, y(2).toFloat)
      })
    //[滚动，滑动]x[时间，计数]+窗口   会话时间窗口
    stream
      .map(x=>(x.id, x.temp, x.timestamp))
      .keyBy(_._1)
      //.window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动时间窗口
      //.window(SlidingProcessingTimeWindows.of(Time.seconds(15), Time.seconds(3))) //滑动时间窗口
      //.window(EventTimeSessionWindows.withGap(Time.seconds(15))) //会话时间窗口
      //.timeWindow(Time.seconds(15)) //简写 滚动时间窗口
      //.timeWindow(Time.seconds(15), Time.seconds(10)) //简写滑动时间窗口
      //.countWindow(3) //滚动计数窗口
      //.countWindow(10, 3) //滑动计数窗口

      //window api
      //.timeWindow(Time.seconds(15)). //triger evictor allowedLatency sideOutputLateData getSideOutput
      //全窗口函数， 增量聚合函数

      .timeWindow(Time.seconds(15))
      //.min(1)
      //.minBy(1)
      .reduce((cur, newData)=>{
        (newData._1, cur._2.min(newData._2), newData._3)
      })
      .print()

    env.execute("window test")
  }
}
case class Sensor8(id:String, timestamp:Long, temp:Double)

//自己实现reduce function
class MyReduceMinFunc extends ReduceFunction[Sensor8] {
  override def reduce(cur: Sensor8, newData: Sensor8): Sensor8 = {
    Sensor8(cur.id, newData.timestamp, cur.temp.min(newData.temp))
  }
}
