package org.tableapi

import org.apache.flink.streaming.api.scala._

object HelloSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fileStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = fileStream.map(x=>{
      val y = x.split(",")
      Sensor(y(0), y(1).toLong, y(2).toDouble)
    })

    stream.print()
    env.execute("table api")
  }
}
case class Sensor(id:String, timestamp:Long, temp:Double)