package org.myflink

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    var originStream = env.socketTextStream("10.227.20.135", 7777)
    var stream = originStream.map(x=>{
      val y = x.split(",")
      Sensor12(y(0), y(1).toLong, y(2).toFloat)
    })

    val highTempStream = stream
      .process(new SplitTempProcessor(30.0))
    highTempStream.print("high")
    highTempStream
      .getSideOutput(new OutputTag[(String, Long, Double)]("low"))
      .print("low")
    env.execute("SideOutput")
  }
}

case class Sensor12(id:String, timestamp:Long, temp:Double)

class SplitTempProcessor(threadHold: Double) extends ProcessFunction[Sensor12, Sensor12]{
  override def processElement(value: Sensor12, ctx: ProcessFunction[Sensor12, Sensor12]#Context, out: Collector[Sensor12]): Unit = {
    if(value.temp > threadHold) {
      out.collect(value)
    }else{
      ctx.output(new OutputTag[(String, Long, Double)]("low"), (value.id, value.timestamp, value.temp) )
    }
  }
}