package org.tableapi

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.table.types._

object SQLStructure {
  def main(args: Array[String]): Unit = {
    /*
      val tableEnv = ... //创建表执行环境
      //创建表，用于读取数据
      tabEnv.connect(kafka..).createTemporaryTable("inputTable")
      //注册表，用于计算结果输出
      tableEnv.connect(...).createTemporaryTable("outputTable")

      //通过tableAPI查询算子，得到一张结果表
      val result = tableEnv.from("inputTable").select(...)

      //通过SQL查询语句，得到结果表
      val sqlResult = tableEnv.sqlQuery("select ... from inputTable ...")

      //将结果表写入到输出表中
      result.insertInto("outputTable")
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tabEnv = StreamTableEnvironment.create(env)
    //老版本planner
    val settingsOld = EnvironmentSettings.newInstance()
      .useOldPlanner()
      .inStreamingMode()
      .build()
    val oldStreamTableEnv = StreamTableEnvironment.create(env, settingsOld)
    //老版本批处理
    val batchEnv = ExecutionEnvironment.getExecutionEnvironment
    val oldBatchTableEnv = BatchTableEnvironment.create(batchEnv)

    //新版本Blink 流处理
    val blinkStreamSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val blinkStreamTabEnv = StreamTableEnvironment.create(env, blinkStreamSettings)

    //新版本Blink 流处理
    val blinkBatchSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inBatchMode()
      .build()
    val blinkBatchTabEnv = TableEnvironment.create(blinkBatchSettings)

    /*
    tableEnv
    .connect(...) //和外部数据源建立连接
    .withForamt(...) //序列化，
    .withSchema(...) //表结构
    .createTemporaryTable("myTable")
     */
    val filePath = "C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt"
    blinkStreamTabEnv
      .connect(new FileSystem().path(filePath))
      //.withFormat(new OldCsv())
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")
    val inputTable = blinkStreamTabEnv.from("inputTable")
    inputTable.toAppendStream[(String, Long, Double)].print("all")
    val sql = "select * from inputTable where temp > 30.0"
    blinkStreamTabEnv
      .sqlQuery(sql)
      .toAppendStream[(String,Long,Double)]
      .print("high")
    env.execute("SQL Structure")
  }
}
case class Sensor1(id:String, timestamp:Long, temp:Double)
