package org.myflink

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state._
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.util
// 算子状态:列表状态ListState 联合列表状态UnionListState 广播状态(BroadcastState)
// 键控状态:值状态ValueState 列表状态ListState 映射状态(MapState) 聚合状态(ReducingState AggregatingState)
object StateCheck {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //内存，文件系统，rocksdb状态后端
    //env.setStateBackend(new MemoryStateBackend())
    //env.setStateBackend(new FsStateBackend("hdfs://xxx"))
    //env.setStateBackend(new RocksDBStateBackend("rocksdb://xxxx")) //可以开启增量存盘
    //开启端口：nc -k -l -p 7777， 输入sensor文件内容
    var originStream = env.socketTextStream("10.227.20.135", 7777)
    var stream = originStream
      .map((x)=>{
        val y = x.split(',')
        Sensor10(y(0), y(1).toLong, y(2).toDouble)
      })
    //温度值跳变超过10，报警
    stream
      .keyBy(_.id)
      .flatMap(new TempChangeFlatMap(10))
      .print("alert")

    //温度值跳变超过10，报警
    stream
      .keyBy(_.id)
      .flatMapWithState[(String,Double,Double), Double]{
        case (data:Sensor10, None) => (List.empty, Some(data.temp))
        case (data:Sensor10, lastTemp:Some[Double]) =>{
          val diff = (data.temp - lastTemp.get).abs
          if(diff > 10.0 ){
            (List((data.id, data.temp, lastTemp.get)), Some(data.temp))
          }else{
            (List.empty, Some(data.temp))
          }
        }
      }
      .print("alert2")
    env.execute("state test")
  }
}

case class Sensor10(id:String, timestamp:Long, temp:Double)
class MyReducer extends ReduceFunction[Sensor10] {
  override def reduce(currState: Sensor10, newData: Sensor10): Sensor10 = Sensor10(currState.id, newData.timestamp, currState.temp.min(newData.temp))
}
class TempChangeFlatMap(threadHold:Int) extends RichFlatMapFunction[Sensor10,(String, Double, Double)]{
  lazy val lastTemp = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  override def flatMap(value: Sensor10, out: Collector[(String,Double,Double)]): Unit = {
    val temp = lastTemp.value()
    val diff = (value.temp - temp).abs
    if (diff > threadHold) {
      out.collect((value.id + " 温度跳变超过阈值", value.temp, temp))
    }
    lastTemp.update(value.temp)
  }
}
class MyRichMapper1 extends RichMapFunction[Sensor10, String] {
  var valueState:ValueState[Double] = _
  //方式2，运行起来，对象创建后再get
  //lazy val valueState2 = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))
  lazy val listState = getRuntimeContext.getListState(new ListStateDescriptor[Double]("listState", classOf[Double]))
  lazy val mapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("mapState", classOf[String], classOf[Double]))
  lazy val reducingState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Sensor10]("reducingState", new MyReducer(), classOf[Sensor10]))
  override def map(value: Sensor10): String = {
    //获取，保存状态
    val myValue = valueState.value()
    valueState.update(value.temp)
    //list state
    listState.add(1)
    val lst = new util.ArrayList[Double]()
    lst.add(3)
    listState.addAll(lst)
    listState.update(lst)
    listState.get()
    //map state
    val v = mapState.get(value.id)
    mapState.contains(value.id)
    mapState.put("sensor_1", 1)
    //reduce
    reducingState.add(value)
    reducingState.get()
    value.id
  }

  override def open(parameters: Configuration): Unit = {
      valueState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("valueState", classOf[Double]))

    }
}