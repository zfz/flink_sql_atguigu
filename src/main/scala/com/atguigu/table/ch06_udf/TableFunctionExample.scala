package com.atguigu.table.ch06_udf

import com.atguigu.table.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("src/main/resources/sensor.txt")
    // val inputStream: DataStream[String] = env.socketTextStream("localhost", 7777)

    // 2. 转换成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val dataArray = data.split(",")
        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
      })
      // 分配时间戳和Watermark
      .assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
          override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
        } )

    // 3. 将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 4. 先创建一个UDF对象
    val split = new Split("_")

    // 5.1 Table API调用
    val resultTable: Table = sensorTable
      // 侧向连接，应用TableFunction，也可以使用leftOuterJoinLateral
      .joinLateral(split('id) as ('word, 'length))
      .select('id, 'ts, 'word, 'length)

    // 5.2 SQL调用
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("split", split)
    val resultSQLTable = tableEnv.sqlQuery(
      """
        | select id, ts, word, length
        | from sensor, lateral table(split(id)) as splitid(word, length)
      """.stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSQLTable.toAppendStream[Row].print("sql result")

    env.execute("Table Function Example")
  }
}

/**
 * 自定义TableFunction，实现分割字符串并统计长度(word, length)
 * 一行数据拆出多行
 */
class Split(separator: String) extends TableFunction[(String, Int)] {
  // 返回值不能为Unit，使用collector收集数据
  def eval(value: String): Unit = {
    value.split(separator).foreach(
      word => collect(word, word.length)
    )

  }
}