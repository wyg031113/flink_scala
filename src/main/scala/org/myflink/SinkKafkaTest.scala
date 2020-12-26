package org.myflink

import org.apache.flink.api.common.serialization.{SimpleStringEncoder, SimpleStringSchema}
import org.apache.flink.streaming.api.environment._
import org.apache.flink.streaming.connectors.kafka._

object SinkKafkaTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamOrigin = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = streamOrigin.map(x=>{
      val y=x.split(",")
      Sensor4(y(0), y(1).toLong, y(2).toDouble).toString
    })

    stream.addSink(new FlinkKafkaProducer011[String]("10.227.20.135:9092", "sensor_sink", new SimpleStringSchema()))
  env.execute("kafka sink")
  }
}
case class Sensor4(id:String, timestamp:Long, temp:Double)