package org.myflink

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6._
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object ESSink {
  def main(args: Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fileStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = fileStream
      .map((x)=>{
        val y = x.split(",")
        Sensor6(y(0),y(1).toLong, y(2).toDouble)
      })
    val httpAddr = new java.util.ArrayList[HttpHost]()
    httpAddr.add(new HttpHost("10.227.20.135",9200))
    val esSink = new ElasticsearchSink.Builder[Sensor6](httpAddr, new MyESSink ).build()
    stream.addSink(esSink)
    env.execute("es es")
  }
}

case class Sensor6(id:String, timestamp:Long, temp:Double)
class MyESSink extends ElasticsearchSinkFunction[Sensor6] {
  override def process(t: Sensor6, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    val dataSource = new java.util.HashMap[String, String]()
    dataSource.put("id", t.id)
    dataSource.put("temp", t.temp.toString)
    dataSource.put("ts", t.timestamp.toString)
    val indexRequest = Requests.indexRequest()
      .index("sensor")
      .`type`("readingdata")
      .source(dataSource)
    requestIndexer.add(indexRequest)
  }
}