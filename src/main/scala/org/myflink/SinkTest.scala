package org.myflink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

object SinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamOrigin = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = streamOrigin.map(x=>{
      val y=x.split(",")
      Sensor3(y(0), y(1).toLong, y(2).toDouble)
    })

    //并行度为1就生成文件，负责生成目录，目录里再写多个文件
    stream.writeAsCsv("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor_sink.csv")

    stream.addSink(StreamingFileSink.forRowFormat(
      new Path("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor_sink_2.csv"),
      new SimpleStringEncoder[Sensor3]()
    ).build())
    env.execute("sink test")
  }
}
case class Sensor3(id:String, timestamp:Long, temp:Double)

