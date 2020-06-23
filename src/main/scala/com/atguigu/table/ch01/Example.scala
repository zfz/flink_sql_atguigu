package com.atguigu.table.ch01

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.Table

object Example {
  def main(args: Array[String]): Unit = {
    // 0. 创建流执行环境，读取数据并转换成样例类类型
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 从文件读取数据
    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt")
    // val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })

    // 1. 基于env创建表环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2. 基于tableEnv，将流转换成表
    val sensorTable: Table = tableEnv.fromDataStream(dataStream)

    // 3. 转换操作，得到提取结果
    // 3.1 调用Table API，做转换操作
    val resultTable: Table = sensorTable.select("id, temperature").filter("id == 'sensor_1'")
    // 3.2 SQL实现转换
    tableEnv.createTemporaryView("sensor_tbl", sensorTable)
    val resultSQLTable: Table = tableEnv.sqlQuery(
      """
        | select id, temperature
        | from sensor_tbl
        | where id = 'sensor_1'
      """.stripMargin)

    // 4. 把表转换成流，打印输出
    // val resultStream: DataStream[(String, Double)] = resultTable.toAppendStream[(String, Double)]
    val resultStream: DataStream[(String, Double)] = resultSQLTable.toAppendStream[(String, Double)]

    resultStream.print()

    env.execute("Table API Example")
  }
}
