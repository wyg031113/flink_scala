package org.myflink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import java.util.Properties
import scala.util.Random

object SourceTest {
  def main(args: Array[String]): Unit = {
    var env = StreamExecutionEnvironment.getExecutionEnvironment
    //从集合里读取数据
    var dataList = List(
      Sensor("sensor_1", 1547718199, 35.8),
      Sensor("sensor_6", 1547718201, 15.4),
      Sensor("sensor_7", 1547718202, 6.7),
      Sensor("sensor_10", 1547718205, 38.1),
    )

    var dataStream = env.fromCollection(dataList)
    dataStream.print

    var stream2 = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    def Line2Sensor(line:String):Sensor={
      var words = line.split(",")
      return Sensor(words(0), words(1).toInt, words(2).toDouble)
    }
    stream2
      .map(Line2Sensor _)
      .print
    //从kafka读取数据 kafka生产：./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
    var property = new Properties()
    property.setProperty("bootstrap.servers","10.227.20.135:9092")
    property.setProperty("group.id", "consumer-group")
    var stream3 = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(),property))
    stream3.print()
    //自定义数据源
    var stream4 = env.addSource(new MySensorSource())
    stream4.print()
    env.execute()
  }
}

case class Sensor(id: String, timestamp: Long, temp: Double)

class MySensorSource extends SourceFunction[Sensor] {
  var stop = false
  override def run(ctx: SourceFunction.SourceContext[Sensor]): Unit = {
    val rand = new Random()
    var curTemp = 1.to(10).map(i=>("sensor"+i.toString, rand.nextDouble()*100))
    while(!stop){
      curTemp =  curTemp.map(
        data=>(data._1, data._2 + rand.nextGaussian())
      )
      val curTimeStamp = System.currentTimeMillis()
      curTemp.foreach(data=>ctx.collect(Sensor(data._1, curTimeStamp, data._2)))
      Thread.sleep(2000)
    }
  }

  override def cancel(): Unit = stop = true
}