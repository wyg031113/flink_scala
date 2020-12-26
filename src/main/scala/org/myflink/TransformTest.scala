package org.myflink

import org.apache.flink.api.common.functions.{FilterFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val streamOrigin = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    //简单求最小值
    val stream = streamOrigin
      .map(x=>{
        val y = x.split(",")
        Sensor2(y(0), y(1).toLong, y(2).toFloat)
      })
      stream
      .keyBy("id")
      .minBy("temp") //最小值，返回sensor;  min只会用第一个数据的字段，只会更新temp字段
      .print("min")

    //返回temp最小值， timestamp返回最近的那条数据的时间戳， 最近哪条数据可能temp比当前小
    stream
      .keyBy("id")
      .reduce((currState, newData)=>Sensor2(currState.id, newData.timestamp, currState.temp.min(newData.temp)))
      .print("RD")

    //使用ReduceFunctionl来解决问题
    stream
      .keyBy("id")
      .reduce(new MyReduceFunction)
      .print("RF")

    //分流操作split
    val splitStream = stream
    .split(d=>{if(d.temp>=30) Seq("high") else Seq("low")})
    //选择流，可以多个参数
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("low", "high")
    high.print("high")
    low.print("low")
    all.print("all")

    //合流
    val warningStream = high.map(x=>(x.id, x.temp))
    val connectedStream = warningStream.connect(low)
    connectedStream
      .map(warn=>{(warn._1, warn._2, "warn")}, low=>{(low.id, "health")})
      .print("cs")

    //union合流, 可以union多个
    val unionStream = low.union(high)
    unionStream.print("union")

    //UDF函数，比如用个filter
    stream.filter(new MyFileterFunction).print("Filter")

    //Rich函数,可以获取运行上下文和生命周期函数
    stream.map(new MyRichMapper).print("RichMap")
    env.execute("transform test")
  }
}
case class Sensor2(id: String, timestamp: Long, temp: Double)

class MyReduceFunction extends ReduceFunction[Sensor2] {
  override def reduce(currState: Sensor2, newData: Sensor2): Sensor2 = Sensor2(currState.id, newData.timestamp, currState.temp.min(newData.temp))
}

class MyFileterFunction extends  FilterFunction[Sensor2] {
  override def filter(value: Sensor2): Boolean = value.id.startsWith("sensor_1")
}

class MyRichMapper extends  RichMapFunction[Sensor2, String] {
  override def open(parameters: Configuration): Unit = {
    //做初始化连接，比如连接数据库，在类对象创建时调用一次，

    //还能获得运行上下文
    //getRuntimeContext().getBroadcastVariableWithInitializer()
  }

  override def close(): Unit = {
    //关闭数据库连接
  }
  override def map(value: Sensor2): String = value.id+" temp"
}