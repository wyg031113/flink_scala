package org.myflink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
//ProcessFuncion
/*
ProcessFunction
KeyedProcessFunction
ProcessWindowFunction
CoProcessFunction
ProcessJoinFunction
BroadcastProcessFunction
KeyedBroadcastProcessFunction
ProcessAllWindowFunction
 */
object ProcessFuncTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val origin = env.socketTextStream("10.227.20.135", 7777)
    val stream = origin.map(x=>{
      val y = x.split(",")
      Sensor11(y(0),y(1).toLong, y(2).toDouble)
    })

    stream
      .keyBy(_.id)
      .process(new MyKeyProcessFunction)
    stream
      .keyBy(_.id)
      .process(new TempIncKeyedProcessFunction(10000))
      .print("alert incr")
    env.execute("process function")
  }
}
case class Sensor11(id:String, timestamp:Long, temp:Double)
class TempIncKeyedProcessFunction(interval: Int) extends  KeyedProcessFunction[String, Sensor11, String] {
  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  lazy val timerState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer", classOf[Long]))
  override def processElement(value: Sensor11, ctx: KeyedProcessFunction[String, Sensor11, String]#Context, out: Collector[String]): Unit = {
    val lastTemp = lastTempState.value()
    val timer = timerState.value()
    if(timer == 0){
      val timer = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }else{
      if(value.temp < lastTemp){
        ctx.timerService().deleteProcessingTimeTimer(timer)
        timerState.clear()
      }
    }
    lastTempState.update(value.temp)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor11, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect(ctx.getCurrentKey + "温度连续上升了" + interval.toString+"毫秒")
  }

}
class MyKeyProcessFunction extends  KeyedProcessFunction[String, Sensor11, String]{
  override def processElement(value: Sensor11, ctx: KeyedProcessFunction[String, Sensor11, String]#Context, out: Collector[String]): Unit = {
    ctx.getCurrentKey
    ctx.timestamp
    ctx.timerService().currentWatermark()
    ctx.timerService().currentProcessingTime()
    ctx.timerService().registerEventTimeTimer(ctx.timestamp()+6000)
    //ctx.timerService().deleteEventTimeTimer(ctx.timestamp()+6000)
  }
  var myState: ValueState[Int] = _
  override def open(parameters: Configuration): Unit = {
    myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("myState", classOf[Int]))
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, Sensor11, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}