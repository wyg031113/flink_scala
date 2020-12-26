package org.myflink

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    //创建执行环境
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val inputPath:String = "C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\hello.txt"
    val inputDataSet:DataSet[String] = env.readTextFile(inputPath)
    val resultDataSet:DataSet[(String,Int)] = inputDataSet
      .flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
    resultDataSet.print()
  }
}
