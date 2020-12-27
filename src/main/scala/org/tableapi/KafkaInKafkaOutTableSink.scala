package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object KafkaInKafkaOutTableSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    //kafka生产值： ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor1
    //注册kafka input 表
    tabEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("sensor1")
        .property("zookeeper.connect","10.227.20.135:2181")
        .property("bootstrap.servers", "10.227.20.135:9092")
      )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")
    //注册kafka output 表
    tabEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("sensor2")
        .property("zookeeper.connect","10.227.20.135:2181")
        .property("bootstrap.servers", "10.227.20.135:9092")
      )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("outputTable")

    //转换操作
    val sensorTable = tabEnv.from("inputTable")
    val resultTable = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    val aggTable = sensorTable
      .groupBy('id)
      .select('id, 'id.count as 'cnt)

    //输出到控制台
    resultTable
      .toAppendStream[(String, Double)]
      .print("result")

    aggTable
      .toRetractStream[(String, Long)]
      .print("agg")

    //输出到kafka
    resultTable.insertInto("outputTable")
    //kafka不支持更新，故不能insert
    //aggTable.insertInto("outputTable2")
    env.execute("kafka in out pipline")
  }
}
