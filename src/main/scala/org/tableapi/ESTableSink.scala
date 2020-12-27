package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object ESTableSink {
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
      .groupBy('id)  //这里相当于指定了ES的Key
      .select('id, 'id.count as 'cnt)

    resultTable
      .toAppendStream[(String, Double)]
      .print("result")

    aggTable
      .toRetractStream[(String, Long)]
      .print("agg")


    //输出到ES
    tabEnv
      .connect(new Elasticsearch()
        .version("6")
        .host("10.227.20.135", 9200, "http")
        .index("sensor_es")
        .documentType("temperature")
        )
      .inUpsertMode()
      .withFormat(new Json)
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("cnt", DataTypes.BIGINT())
      )
      .createTemporaryTable("outputTable")
    //插入数据，可以修改的
    aggTable.insertInto("outputTable")
    env.execute("es table sink")
  }
}
/*
~/software/kafka_2.12-2.6.0$ curl "localhost:9200/_cat/indices?v"
health status index                          uuid                   pri rep docs.count docs.deleted store.size pri.store.size
yellow open   sensor_es                      oFmw1hMdR1-QF62LXn6d6Q   1   1          4           10     10.5kb         10.5kb
~/software/kafka_2.12-2.6.0$ curl "localhost:9200/sensor_es/_search?pretty"
{
  "took" : 15,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 4,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "sensor_es",
        "_type" : "temperature",
        "_id" : "sensor_1",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_1",
          "cnt" : 8
        }
      },
      {
        "_index" : "sensor_es",
        "_type" : "temperature",
        "_id" : "sensor_6",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_6",
          "cnt" : 2
        }
      },
      {
        "_index" : "sensor_es",
        "_type" : "temperature",
        "_id" : "sensor_7",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_7",
          "cnt" : 2
        }
      },
      {
        "_index" : "sensor_es",
        "_type" : "temperature",
        "_id" : "sensor_10",
        "_score" : 1.0,
        "_source" : {
          "id" : "sensor_10",
          "cnt" : 2
        }
      }
    ]
  }
}
 */