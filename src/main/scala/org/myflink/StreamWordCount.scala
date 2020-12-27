package org.myflink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val paramTool =ParameterTool.fromArgs(args)
    val host = paramTool.get("host")
    val port = paramTool.getInt("port")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(2)
    //接收socket文本流
    //开启端口：nc -k -l -p 7777， 输入sensor文件内容
    val inputDataStream:DataStream[String] = env.socketTextStream(host, port)
    val resultDataStream = inputDataStream
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(0)
      .sum(1)
    resultDataStream.print().setParallelism(1)
    //启动任务执行
    env.execute("stream word count")
  }
}
