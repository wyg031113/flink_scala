package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object HelloSQL {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fileStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = fileStream.map(x=>{
      val y = x.split(",")
      Sensor(y(0), y(1).toLong, y(2).toDouble)
    })
    stream.print("origin")

    //TableAPI使用流程
    //创建table执行环境
    val tabEnv = StreamTableEnvironment.create(env)
    val dataTable:Table = tabEnv.fromDataStream(stream)

    val result = dataTable
      .select("id, temp")
      .filter("id = 'sensor_1'")
    result
      .toAppendStream[(String, Double)]
      .print("inner")

    //直接用SQL
    //注册table
    tabEnv.createTemporaryView("datatable",dataTable)
    val sql = "select id, temp from datatable where id='sensor_1'"
    val resultSql = tabEnv.sqlQuery(sql)
    resultSql.toAppendStream[(String,Double)].print("SQL")
    env.execute("table api")
  }
}
case class Sensor(id:String, timestamp:Long, temp:Double)