package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object ProctimeTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fileStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = fileStream.map(x=>{
      val y = x.split(",")
      Sensor2(y(0), y(1).toLong, y(2).toDouble)
    })
    stream.print("origin")

    //TableAPI使用流程
    //创建table执行环境
    val tabEnv = StreamTableEnvironment.create(env)
    val dataTable:Table = tabEnv.fromDataStream(stream, 'id, 'temp, 'timestamp, 'pt.proctime)
    dataTable.printSchema()
    dataTable.toAppendStream[Row].print("proctime")
    env.execute("table proctime")
  }
}
case class Sensor2(id:String, timestamp:Long, temp:Double)