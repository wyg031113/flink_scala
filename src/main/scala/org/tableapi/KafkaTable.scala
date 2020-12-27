package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object KafkaTable {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    //kafka生产值： ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
    tabEnv
      .connect(new Kafka()
        .version("0.11")
        .topic("sensor")
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
    val stream = tabEnv.from("inputTable")
    stream
      .toAppendStream[(String, Long,Double)]
      .print("stream")
    tabEnv
      .sqlQuery("select * from inputTable")
      .toAppendStream[(String,Long,Double)]
      .print("sql")
    env.execute("kafka table")
  }
}
/*
~/software/kafka_2.12-2.6.0$ ./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic sensor
>sensor_1,1547718199,35.8
>sensor_6,1547718201,15.4
sensor_7,1547718202,6.7
s>>ensor_10,1547718205,38.1
 */