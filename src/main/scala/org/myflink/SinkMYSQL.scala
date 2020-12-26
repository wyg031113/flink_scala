package org.myflink


import java.sql.{Connection, PreparedStatement}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

import java.sql.DriverManager


object MySQLSink {
  def main(args: Array[String]):Unit={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fileStream = env.readTextFile("C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt")
    val stream = fileStream
      .map((x)=>{
        val y = x.split(",")
        Sensor7(y(0),y(1).toLong, y(2).toDouble)
      })

    stream.addSink(new MyJDBCSink)
    env.execute("es es")
  }
}
case class Sensor7(id:String, timestamp:Long, temp:Double)
class MyJDBCSink extends RichSinkFunction[Sensor7] {
  var conn:Connection = _
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _
  override def invoke(s: Sensor7, context: SinkFunction.Context[_]): Unit = {
    updateStmt.setDouble(1, s.temp)
    updateStmt.setString(2, s.id)
    updateStmt.execute()
    if(updateStmt.getUpdateCount == 0) {
      insertStmt.setString(1, s.id)
      insertStmt.setDouble(2, s.temp)
      insertStmt.execute()
    }
  }

  override def open(parameters: Configuration): Unit = {
    conn = DriverManager.getConnection("jdbc:mysql://10.227.20.135/test","wyg","1503121660")
    insertStmt = conn.prepareStatement("insert into sensor(id, temp) values(?,?)")
    updateStmt = conn.prepareStatement("update sensor set temp = ? where id = ?")
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}