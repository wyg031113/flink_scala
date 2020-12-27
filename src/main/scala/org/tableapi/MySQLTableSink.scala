package org.tableapi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._
import org.apache.flink.types.Row

object MySQLTableSink {
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
      .groupBy('id) //这里相当于指定了ES的Key
      .select('id, 'id.count as 'cnt)

    resultTable
      .toAppendStream[(String, Double)]
      .print("result")

    aggTable
      .toRetractStream[(String, Long)]
      .print("agg")
    aggTable
      .toRetractStream[Row]
      .print("row")
    //与mysql连接
    val sinkDDL =
      """
        |create table jdbcOutputTable(
        | id varchar(30) not null,
        | cnt bigint not null
        | ) with (
        |   'connector.type' = 'jdbc',
        |   'connector.url' = 'jdbc:mysql://10.227.20.135:3306/test',
        |   'connector.table' = 'sensor_count',
        |   'connector.driver' = 'com.mysql.jdbc.Driver',
        |   'connector.username' = 'wyg',
        |   'connector.password' = '1503121660'
        |)
        |""".stripMargin

    tabEnv.sqlUpdate(sinkDDL)
    aggTable.insertInto("jdbcOutputTable")
    //查看执行计划
    val explainPlan = tabEnv.explain(aggTable)
    println(explainPlan)
    env.execute("mysql table sink")
  }
}

/*
MariaDB [(none)]> use test
Reading table information for completion of table and column names
You can turn off this feature to get a quicker startup with -A

Database changed
MariaDB [test]> create table sensor_count(id varchar(30) not null, cnt bigint not null);
Query OK, 0 rows affected (0.07 sec)

MariaDB [test]> select * from sensor_count;
Empty set (0.00 sec)

MariaDB [test]> select * from sensor_count;
+-----------+-----+
| id        | cnt |
+-----------+-----+
| sensor_1  |   8 |
| sensor_10 |   2 |
| sensor_6  |   2 |
| sensor_7  |   2 |
+-----------+-----+
4 rows in set (0.00 sec)

MariaDB [test]>
 */