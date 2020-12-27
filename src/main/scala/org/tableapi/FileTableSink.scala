package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
/*
  更新模式：
  追加：Append
  撤回：Retract  撤回再更新，两条消息
  更新插入：Upsert 只发一条消息，但是需要key
 */
object FileTableSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tabEnv = StreamTableEnvironment.create(env)
    val filePath = "C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor.txt"
    tabEnv
      .connect(new FileSystem().path(filePath))
      .withFormat(new Csv)
      .withSchema(new Schema()
          .field("id", DataTypes.STRING())
          .field("timestamp", DataTypes.BIGINT())
          .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    //转换操作
    val sensorTable = tabEnv.from("inputTable")
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    resultTable
      .toAppendStream[(String, Double)]
      .print("result")

    aggTable
      .toRetractStream[(String, Long)]
      .print("agg")

    //输出到文件
    //注册输出表
    val fileOutPath = "C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor_out.txt"
    tabEnv
      .connect(new FileSystem().path(fileOutPath))
      .withFormat(new Csv)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")
    resultTable.insertInto("outputTable")

      //有过更新操作就不能写文件了
//    val fileOutPath2 = "C:\\Users\\huangyan\\IdeaProjects\\FlinkMaven\\src\\main\\resources\\sensor_out2.txt"
//    tabEnv
//      .connect(new FileSystem().path(fileOutPath2))
//      .withFormat(new Csv)
//      .withSchema(new Schema()
//        .field("id", DataTypes.STRING())
//        .field("cnt", DataTypes.BIGINT())
//      )
//      .createTemporaryTable("outputTable2")
//
//    aggTable
//      .insertInto("outTable2")
    env.execute("file table sink")
  }
}
