package org.myflink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WaterMarkTest {
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //从数据中提取时间
    //env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime) //使用flink处理时间
    env.getConfig.setAutoWatermarkInterval(500) //设置默认watermark间隔
    //var originStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    var originStream = env.socketTextStream("10.227.20.135", 7777)
    var stream = originStream
      .map((x)=>{
        val y = x.split(",")
        Sensor9(y(0), y(1).toLong, y(2).toFloat)
      })
      //.assignAscendingTimestamps(_.timestamp*1000) //升序数据提取时间戳, 并作为watermark,没有时间延时
      //周期性产生watermark  间断生成：数据来了就生成

      //有界乱序时间戳提取器， 最大乱序时间：设置比较小30ms，来hold大部分数据。 用allowedLatency 1min来处理迟到数据。
      //最后用侧输出流处理>1min迟到数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Sensor9](Time.milliseconds(3000)) {
        override def extractTimestamp(element: Sensor9): Long = element.timestamp*1000
      })
    val lateTag = new OutputTag[(String, Double, Long)]("late")
    val resultStream = stream
      .map(x=>(x.id, x.temp, x.timestamp))
      .keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .allowedLateness(Time.minutes(1))
      .sideOutputLateData(lateTag)
      .reduce((cur, newData)=>{
        (newData._1, cur._2.min(newData._2), newData._3)
      })
    resultStream.print("Result")
    resultStream.getSideOutput(lateTag).print("Late")
    env.execute("watermark test")
    //窗口边界起始位置：timestamp-(timestamp+windowSize)%windowSize  timestamp是第一个数据的时间
  }
}
case class Sensor9(id:String, timestamp:Long, temp:Double)
